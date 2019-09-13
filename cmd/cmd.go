package cmd

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/gertd/go-scp/events"
	"github.com/gertd/go-scp/events/csv"
	"github.com/gertd/go-scp/events/json"
	"github.com/gertd/go-scp/ingest"
	"github.com/gertd/go-scp/scp"
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

var (
	client *scp.Client
	config struct {
		filename   string
		tenant     string
		hostname   string
		source     string
		sourcetype string
	}
)

// Execute -- execute main command handler
func Execute() error {

	var rootCmd = &cobra.Command{
		Use:      "scp-load",
		Short:    "scp data loader",
		Version:  "0.0.1",
		PreRunE:  preRunCmd,
		RunE:     execCmd,
		PostRunE: postRunCmd,
		Args:     cobra.ExactArgs(1),
	}

	rootCmd.Flags().StringP(tenantArg, "", "", "tenant identifier")
	rootCmd.Flags().StringP(hostArg, "", "", "event host name")
	rootCmd.Flags().StringP(sourceArg, "", "", "event source")
	rootCmd.Flags().StringP(sourcetypeArg, "", "", "event sourcetype")

	rootCmd.MarkFlagRequired(tenantArg)

	return rootCmd.Execute()
}

func preRunCmd(cmd *cobra.Command, args []string) error {

	var err error
	config.filename = args[0]
	if _, err := os.Stat(config.filename); os.IsNotExist(err) {
		return fmt.Errorf("file [%s] does not exist", config.filename)
	}

	if config.tenant, err = cmd.Flags().GetString(tenantArg); err != nil {
		return err
	}
	if config.hostname, err = cmd.Flags().GetString(hostArg); err != nil {
		return err
	}
	if config.source, err = cmd.Flags().GetString(sourceArg); err != nil {
		return err
	}
	if config.sourcetype, err = cmd.Flags().GetString(sourcetypeArg); err != nil {
		return err
	}

	var appreg scp.AppReg
	if err = appreg.Load("./appreg.json"); err != nil {
		return err
	}

	client = scp.NewClient(config.tenant, appreg.ClientID, appreg.ClientSecret)
	if err := client.Authenticate(); err != nil {
		return err
	}

	return nil
}

func execCmd(cmd *cobra.Command, args []string) error {

	r, err := os.Open(config.filename)
	if err != nil {
		return err
	}
	defer r.Close()

	buf := bufio.NewReader(r)

	sigs := make(chan os.Signal, 1)
	quit := make(chan bool)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Println(sig)
		quit <- true
	}()

	// produce ingest events
	var ep events.Producer

	if filepath.Ext(config.filename) == jsonExt {
		ep = json.NewEventsProducer(quit)
	} else if filepath.Ext(config.filename) == csvExt {
		ep = csv.NewEventsProducer(quit)
	} else {
		ep = json.NewEventsProducer(quit)
	}
	log.Printf("Selected event producer [%T]", ep)

	props := events.Properties{
		Host:       &config.hostname,
		Source:     &config.source,
		Sourcetype: &config.sourcetype,
	}
	go ep.Run(buf, props)

	// consume ingest events + produce batch evenys
	bp := ingest.NewBatchProcessor(ep.Events(), quit)
	go bp.Run()

	// consume batch events
	bw := ingest.NewBatchWriter(client, bp.Batches(), quit)
	bw.Run()

	log.Printf("Summary: batches %d events %d size %d\n", bw.TotalBatches(), bp.TotalEvents(), bp.TotalByteSize())

	return nil
}

func postRunCmd(cmd *cobra.Command, args []string) error {

	return nil
}
