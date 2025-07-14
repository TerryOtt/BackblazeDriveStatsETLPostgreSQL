package main

import (
	"fmt"
	"sync"
)

func readCsvFiles(csvChannel chan string,
	datapointsChannel chan string,
	wg *sync.WaitGroup) {

	fmt.Println("Waiting for CSV filenames over channel")

	// Read from channel until it is closed
	for csvFile := range csvChannel {
		fmt.Printf("Reading CSV file %s\n", csvFile)
	}

	// Have to mark all our work is done before we bail
	wg.Done()
}

func main() {
	fmt.Println("Parsing cmdline args")
	// Create channel to send CSV to readers
	var csvWg sync.WaitGroup
	csvFilenamesChannel := make(chan string)

	// Create channel to send datapoints to writers
	const (
		// CHANNEL_SIZE 512 MB of datapoints at 400 bytes/datapoint (roughly)
		ChannelSize int = (512 * 1024 * 1024) / 400
	)
	datapointsChannel := make(chan string, ChannelSize)

	fmt.Println("Starting reader workers")
	const NumReaders int = 1
	for i := 0; i < NumReaders; i++ {
		csvWg.Add(1)
		go readCsvFiles(csvFilenamesChannel, datapointsChannel, &csvWg)
	}
	fmt.Println("Starting writer workers")
	fmt.Println("Finding CSV files")
	// Send all CSV files over the channel to the readers
	csvFilenamesChannel <- "somefile.csv"

	// Close the channel to bring readers home
	close(csvFilenamesChannel)
	fmt.Println("CSV reads have kicked off")
	// Wait on reader waitgroup
	csvWg.Wait()
	fmt.Println("All datapoints have been read, waiting for writes to complete")
	// Poison pill the writers
	// Wait for writers waitgroup to finish
	fmt.Println("All datapoints have been successfully written")
}
