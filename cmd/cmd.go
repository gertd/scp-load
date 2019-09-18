package cmd

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/gertd/go-scp/events"
	"github.com/gertd/go-scp/events/csv"
	"github.com/gertd/go-scp/events/json"
	"github.com/gertd/go-scp/ingest"
	"github.com/gertd/go-scp/scp"
	"github.com/spf13/cobra"
)

// ldflags injected build version info
var (
	version string //nolint:gochecknoglobals
	date    string //nolint:gochecknoglobals
	commit  string //nolint:gochecknoglobals
)

const (
	tenantArg     = "tenant"
	hostArg       = "host"
	sourceArg     = "source"
	sourcetypeArg = "sourcetype"
)

const (
	csvExt  = ".csv"
	jsonExt = ".json"
)

type appContext struct {
	config       *config
	registration *scp.AppReg
	client       *scp.Client
}

type config struct {
	filename   string
	tenant     string
	hostname   string
	source     string
	sourcetype string
}

func (c *config) set(cmd *cobra.Command, args []string) error {

	var err error

	c.filename = args[0]
	if _, err := os.Stat(c.filename); os.IsNotExist(err) {
		return fmt.Errorf("file [%s] does not exist", c.filename)
	}

	if c.tenant, err = cmd.Flags().GetString(tenantArg); err != nil {
		return err
	}
	if c.hostname, err = cmd.Flags().GetString(hostArg); err != nil {
		return err
	}
	if c.source, err = cmd.Flags().GetString(sourceArg); err != nil {
		return err
	}
	if c.sourcetype, err = cmd.Flags().GetString(sourcetypeArg); err != nil {
		return err
	}
	return nil
}

// Execute -- execute main command handler
func Execute() error {

	var (
		appContext = &appContext{
			config:       &config{},
			registration: &scp.AppReg{},
		}
		rootCmd = &cobra.Command{
			Use:     "scp-load",
			Short:   "scp data loader",
			Version: fmt.Sprintf("%s %s-%s #%s %s ", version, runtime.GOOS, runtime.GOARCH, commit, date),
			PreRunE: preRunCmd(appContext),
			RunE:    runCmd(appContext),
			Args:    cobra.ExactArgs(1),
		}
	)

	rootCmd.Flags().StringP(tenantArg, "", "", "tenant identifier")
	rootCmd.Flags().StringP(hostArg, "", "", "event host name")
	rootCmd.Flags().StringP(sourceArg, "", "", "event source")
	rootCmd.Flags().StringP(sourcetypeArg, "", "", "event sourcetype")

	_ = rootCmd.MarkFlagRequired(tenantArg)

	return rootCmd.Execute()
}

func preRunCmd(app *appContext) func(cmd *cobra.Command, args []string) error {

	return func(cmd *cobra.Command, args []string) error {

		var err error
		if err = app.config.set(cmd, args); err != nil {
			return err
		}

		if err = app.registration.Load("./appreg.json"); err != nil {
			return err
		}

		app.client = scp.NewClient(app.config.tenant, app.registration.ClientID, app.registration.ClientSecret)
		if err := app.client.Authenticate(); err != nil {
			return err
		}

		return nil
	}
}

func runCmd(app *appContext) func(cmd *cobra.Command, args []string) error {

	return func(cmd *cobra.Command, args []string) error {

		r, err := os.Open(app.config.filename)
		if err != nil {
			return err
		}
		defer r.Close()

		rdr := bufio.NewReader(r)

		sigs := make(chan os.Signal, 1)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			sig := <-sigs
			log.Println(sig)
			cancel()
		}()

		props := events.Properties{
			Host:       &app.config.hostname,
			Source:     &app.config.source,
			Sourcetype: &app.config.sourcetype,
		}

		// produce ingest events
		var ep events.Producer
		switch filepath.Ext(app.config.filename) {
		case jsonExt:
			ep = json.NewEventsProducer(ctx, rdr, props)
		case csvExt:
			ep = csv.NewEventsProducer(ctx, rdr, props)
		default:
			ep = json.NewEventsProducer(ctx, rdr, props)
		}
		log.Printf("Selected event producer [%T]", ep)

		// produce ingest events
		go ep.Run()

		// consume ingest events + produce batch evenys
		bp := ingest.NewBatchProcessor(ctx, ep.Events())
		go bp.Run()

		// consume batch events
		bw := ingest.NewBatchWriter(ctx, app.client, bp.Batches())
		bw.Run()

		log.Printf("Summary: batches %d events %d size %d\n", bw.TotalBatches(), bp.TotalEvents(), bp.TotalByteSize())

		return nil
	}
}
