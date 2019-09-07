package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	scp "gitlab.com/d5s/go-scp"
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

	bp := scp.NewBatchProcessor(client)
	bp.Start()

	if err := scp.EventProducerJSON(r, bp.Send); err != nil {
		return err
	}

	bp.Close()

	fmt.Printf("batches %d events %d size %d \n", bp.TotalBatches(), bp.TotalEvents(), bp.TotalByteSize())

	return nil
}

func postRunCmd(cmd *cobra.Command, args []string) error {
	return nil
}

// func produce(filename string, eventChan chan ingest.Event) error {

// 	const (
// 		MiB         = int64(1024 * 1024) // max request size (excl overhead, see https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html)
// 		maxEvents   = int(500)           // max number of events in batch https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html
// 		partKeySize = int64(38)          // size of the partition key PartitionKey: aws.String(uuid.New().String())
// 	)

// 	var (
// 		eventCount int64 // total number of events ingested
// 		batchCount int64 // total number of batches
// 		batchSize  int64 // payload byte size of batch
// 		byteSize   int64 // total number of bytes ingested
// 		events     []ingest.Event
// 	)

// 	f, err := os.Open(filename)
// 	if err != nil {
// 		return err
// 	}
// 	defer f.Close()

// 	dec := json.NewDecoder(f)

// 	t0 := time.Now()

// 	var e interface{}
// 	for dec.More() {

// 		if err := dec.Decode(&e); err != nil {
// 			return err
// 		}

// 		if m, ok := e.(map[string]interface{}); ok {
// 			if _, ok := m["index"]; ok {
// 				continue
// 			}
// 		}

// 		ts := time.Now().UTC().Unix() * 1000 // b := scp.NewBatch(client)
// 		// defer b.Close()

// 		// b.Start()
// 		// b.EventReaderJSON(r)

// 		ns := int32(0)

// 		event := ingest.Event{
// 			Host:       &config.hostname,
// 			Source:     &config.source,
// 			Sourcetype: &config.sourcetype,
// 			Timestamp:  &ts,
// 			Nanos:      &ns,
// 			Body:       e,
// 		}

// 		atomic.AddInt64(&eventCount, int64(1))

// 		// determine size of event payload
// 		b, err := json.Marshal(event)
// 		if err != nil {
// 			return err
// 		}

// 		eventSize := int64(len(b)) // + partKeySize

// 		if batchSize+eventSize < MiB && len(events) < maxEvents {

// 			events = append(events, event)
// 			atomic.AddInt64(&batchSize, eventSize)
// 			atomic.AddInt64(&byteSize, eventSize)

// 		} else {

// 			atomic.AddInt64(&batchCount, int64(1))

// 			log.Printf("send batch number of events %d, size %d\n", len(events), batchSize)

// 			atomic.StoreInt64(&batchSize, 0)

// 			if err := client.IngestEvents(&events); err != nil {
// 				return err
// 			}

// 			events = make([]ingest.Event, 0)
// 		}
// 	}

// 	if len(events) > 0 {

// 		atomic.AddInt64(&batchCount, int64(1))

// 		log.Printf("send batch number of events %d, size %d\n", len(events), batchSize)

// 		atomic.StoreInt64(&batchSize, 0)
// 		if err := client.IngestEvents(&events); err != nil {
// 			return err
// 		}

// 		events = make([]ingest.Event, 0)
// 	}

// 	duration := time.Since(t0)
// 	secs := int64(duration.Seconds())

// 	fmt.Printf("event count   %d\n", eventCount)
// 	fmt.Printf("batch count   %d\n", batchCount)
// 	fmt.Printf("total bytes   %d\n", byteSize)
// 	fmt.Printf("duration secs %d\n", secs)

// 	eventSec := eventCount / secs
// 	batchSec := batchCount / secs
// 	bytesSec := byteSize / secs

// 	fmt.Printf("events/sec    %d\n", eventSec)
// 	fmt.Printf("batches/sec   %d\n", batchSec)
// 	fmt.Printf("bytes/sec     %d\n", bytesSec)

// 	return nil
// }

// func consume() {

// }

func pp(v interface{}) string {
	b, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		return ""
	}
	return string(b)
}

// var wg sync.WaitGroup

// runCtx := NewRunContext()

// sigs := make(chan os.Signal, 1)

// // if you hit CTRL-C or kill the process this channel will
// // get a signal and trigger a shutdown of the publisher
// // which in turn should trigger a each step of the pipeline
// // to exit
// signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
// go waitOnSignal(runCtx, sigs)

// go produce(runCtx)

// wg.Add(2)
// go run(runCtx, &wg)
// go batchWriter(runCtx, &wg)

// wg.Wait()

// fmt.Print("\nSummary\n")
// fmt.Printf(" produced: %d\n", runCtx.sumProduced)
// fmt.Printf("     sent: %d\n", runCtx.sumSent)
