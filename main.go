package main

import (
	"fmt"
	"sync"
)

func readCsvFiles(
	csvChannel chan string,
	datapointChannel chan string,
	wg *sync.WaitGroup) {

	fmt.Println("Waiting for CSV filenames over channel")

	// Read from channel until it is closed
	for csvFile := range csvChannel {
		fmt.Printf("Reader working reading CSV file \"%s\"\n", csvFile)
		// Pretend we got some CSV
		datapointChannel <- "totally legit datapoint"
	}

	// Have to mark all our work is done before we bail
	wg.Done()
}

func queueDatapointForWrite(
	datapointChannel chan string,
	wg *sync.WaitGroup) {

	fmt.Println("Waiting for datapoints to be queued")

	// Read from channel until it's closed
	for datapoint := range datapointChannel {
		fmt.Printf("Writer worker got datapoint: \"%s\"\n", datapoint)
	}

	// All our work is done, note that before we bail
	wg.Done()
}

func main() {
	fmt.Println("Parsing cmdline args")
	// Create channel to send CSV to readers
	var readerWg sync.WaitGroup
	const csvChannelSize int = 16384
	csvFilenamesChannel := make(chan string, csvChannelSize)

	// Create channel to send datapoints to writers
	const (
		// CHANNEL_SIZE 512 MB of datapoints at 400 bytes/datapoint (roughly)
		datapointChannelSize int = (512 * 1024 * 1024) / 400
	)
	datapointsChannel := make(chan string, datapointChannelSize)

	fmt.Println("Starting reader workers")
	const numReaders int = 1
	for i := 0; i < numReaders; i++ {
		readerWg.Add(1)
		go readCsvFiles(csvFilenamesChannel, datapointsChannel, &readerWg)
	}
	fmt.Println("Starting writer workers")
	const numWriters = 1
	var writerWg sync.WaitGroup
	for i := 0; i < numWriters; i++ {
		writerWg.Add(1)
		go queueDatapointForWrite(datapointsChannel, &writerWg)
	}

	fmt.Println("Finding CSV files")
	// Send all CSV files over the channel to the readers
	csvFilenamesChannel <- "somefile.csv"

	// Close the channel to bring readers home
	close(csvFilenamesChannel)
	fmt.Println("CSV reads have kicked off")
	// Wait on reader waitgroup
	readerWg.Wait()
	fmt.Println("All datapoints have been read, waiting for writes to complete")

	// All readers have finished, so datapoint can be closed to signal that
	close(datapointsChannel)

	// Wait until all writers are done
	writerWg.Wait()
	fmt.Println("All datapoints have been successfully written")
}
