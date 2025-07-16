package main

import (
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
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

func parseArgs() map[string]any {

	numCpus := runtime.NumCPU()

	// Have one positional arg (CSV dir) and two optional flags,
	//		-r/--readers and -w/--writers

	numReaders := flag.Int("readers", numCpus, "Number of reader workers, defaults to CPU count")
	numWriters := flag.Int("writers", numCpus, "Number of writer workers, defaults to CPU count")

	// Now parse the args
	flag.Parse()

	// Make sure we got exactly one positional arg
	if flag.NArg() != 1 {
		fmt.Println("Usage: prog [--readers READERS] [--writers WRITERS] <CSV dir>")
		os.Exit(1)
	}

	args := map[string]any{
		"readers": *numReaders,
		"writers": *numWriters,
		"csvDir":  flag.Arg(0),
	}

	return args
}

func getCsvListFromDir(csvDir string) []string {
	fmt.Println("\tCSV dir is " + csvDir)
	rootDir := os.DirFS(csvDir)
	var csvList []string
	csvFound := 0
	fs.WalkDir(rootDir, ".",
		func(path string, d fs.DirEntry, err error) error {
			if filepath.Ext(path) == ".csv" {
				csvList = append(csvList, path)
				csvFound++
			}
			return nil
		})

	fmt.Println("\tNumber of CSV files: " + strconv.Itoa(csvFound))
	return csvList
}

func main() {
	fmt.Println("Parsing cmdline args")
	args := parseArgs()

	fmt.Println("Finding CSV files")
	csvList := getCsvListFromDir(args["csvDir"].(string))

	// Create channel to send CSV to readers
	var csvWg sync.WaitGroup

	// Make sure writer of filenames to this channel never buffers
	csvFilenamesChannel := make(chan string, len(csvList))

	// Create channel to send datapoints to writers
	const (
		// ChannelSize 512 MB of datapoints at 400 bytes/datapoint (roughly)
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
	// Send all CSV files over the channel to the readers
	for _, currCsvFilename := range csvList {
		csvFilenamesChannel <- currCsvFilename
	}

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
