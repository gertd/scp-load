package cmd

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"gitlab.com/d5s/go-scp/events"
	"gitlab.com/d5s/go-scp/ingest"
	"gitlab.com/d5s/go-scp/scp"
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

	rootCmd.Flags().StringP("tenant", "", "", "tenant identifier")
	rootCmd.Flags().StringP("host", "", "", "event host name")
	rootCmd.Flags().StringP("source", "", "", "event source")
	rootCmd.Flags().StringP("sourcetype", "", "", "event sourcetype")

	rootCmd.MarkFlagRequired("tenant")

	return rootCmd.Execute()
}

func preRunCmd(cmd *cobra.Command, args []string) error {

	var err error
	config.filename = args[0]
	if _, err := os.Stat(config.filename); os.IsNotExist(err) {
		return fmt.Errorf("file [%s] does not exist", config.filename)
	}

	if config.tenant, err = cmd.Flags().GetString("tenant"); err != nil {
		return err
	}
	if config.hostname, err = cmd.Flags().GetString("host"); err != nil {
		return err
	}
	if config.source, err = cmd.Flags().GetString("source"); err != nil {
		return err
	}
	if config.sourcetype, err = cmd.Flags().GetString("sourcetype"); err != nil {
		return err
	}

	var appreg scp.AppReg
	if err = appreg.Load("./appreg.json"); err != nil {
		return err
	}

	client = scp.NewClient(config.tenant, appreg.ClientID, appreg.ClientSecret)
	_, err = client.TokenSource.Token()
	if err != nil {
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

	// buf := bufio.NewReader(r)

	sigs := make(chan os.Signal, 1)
	quit := make(chan bool)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Println(sig)
		quit <- true
	}()

	// produce ingest events
	ep := events.NewEventsProducerJSON(quit)
	go ep.Run(r)

	// consume ingest events + produce batch evenys
	bp := ingest.NewBatchProcessor(ep.Events(), quit)
	go bp.Run()

	// consume batch events
	bw := ingest.NewBatchWriter(client, bp.Batches(), quit)
	bw.Run()

	log.Printf("batches %d events %d size %d \n", bw.TotalBatches(), bp.TotalEvents(), bp.TotalByteSize())

	return nil
}

func postRunCmd(cmd *cobra.Command, args []string) error {

	return nil
}
