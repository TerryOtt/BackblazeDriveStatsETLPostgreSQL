package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"
)

type DriveHealthDatapoint struct {
	Date              time.Time
	DriveSerialNumber string
	DriveId           uuid.UUID
	DriveModel        string
	DriveModelId      uuid.UUID
	CapacityBytes     uint64
	Failure           uint8
}

type IdAssignmentCaches struct {
	DriveModelIdsLock          sync.RWMutex
	DriveIdsLock               sync.RWMutex
	DriveModelsToDriveModelIds map[string]uuid.UUID
	DriveSerialNumberToId      map[string]uuid.UUID
}

func addIdValuesToDatapoint(
	caches *IdAssignmentCaches,
	datapoint *DriveHealthDatapoint) {

	// If we know the drive ID, we know the drive model as well
	caches.DriveIdsLock.RLock()
	defer caches.DriveIdsLock.RUnlock()
	driveId, ok := caches.DriveSerialNumberToId[datapoint.DriveSerialNumber]
	if ok {
		// We knew the drive ID, so we know everything
		datapoint.DriveId = driveId
		caches.DriveModelIdsLock.RLock()
		datapoint.DriveModelId = caches.
	}
}

func readCsvFiles(
	idCaches *IdAssignmentCaches,
	csvChannel chan string,
	datapointsChannel chan DriveHealthDatapoint,
	wg *sync.WaitGroup) {

	//fmt.Println("Waiting for CSV filenames over channel")

	// Read from channel until it is closed
	for csvFile := range csvChannel {
		//fmt.Println("\tReading CSV file ", csvFile)
		fileHandle, err := os.Open(csvFile)
		if err != nil {
			panic(err)
		}
		defer fileHandle.Close()
		csvReader := csv.NewReader(fileHandle)
		// Skip header row
		_, err = csvReader.Read()
		if err != nil {
			panic(err)
		}

		// Now read all the datapoints and stream them to datapoints channel
		for {
			csvRow, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}
			// Parse date
			datapointDate, err := time.Parse(time.DateOnly, csvRow[0])
			if err != nil {
				panic(err)
			}

			// Int conversions on capacity bytes and failure
			capacityBytes, err := strconv.ParseUint(csvRow[3], 10, 64)
			if err != nil {
				panic(err)
			}
			failure, err := strconv.ParseUint(csvRow[4], 10, 8)
			if err != nil {
				panic(err)
			}

			// Create datapoint struct
			newDatapoint := DriveHealthDatapoint{
				Date:              datapointDate,
				DriveSerialNumber: csvRow[1],
				DriveId:           uuid.Nil,
				DriveModel:        csvRow[2],
				DriveModelId:      uuid.Nil,
				CapacityBytes:     capacityBytes,
				Failure:           uint8(failure),
			}
			addIdValuesToDatapoint(idCaches, &newDatapoint)
			// TODO: assign unique drive model and drive ID's
			datapointsChannel <- newDatapoint

		}
	}

	// Have to mark all our work as a CSV reader is done before we bail
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
				fullPath := filepath.Join(csvDir, path)
				csvList = append(csvList, fullPath)
				csvFound++
			}
			return nil
		})

	fmt.Println("\tNumber of CSV files: " + strconv.Itoa(csvFound))
	return csvList
}

func writerWorker(datapointChannel chan DriveHealthDatapoint, wg *sync.WaitGroup) {
	//fmt.Println("Entering writer worker")

	// Read from channel until it's both empty AND closed
	datapointsReadFromChannel := 0
	for _ = range datapointChannel {
		//fmt.Println("\t\tWriter worker got datapoint: ", currDatapoint)
		datapointsReadFromChannel++
	}

	fmt.Println("\tWriter saw this many datapoints: ", datapointsReadFromChannel)
	// Mark that we've done our work to let the waitgroup proceed towards
	//		unblocking
	wg.Done()
}

func main() {
	fmt.Println("Parsing cmdline args")
	args := parseArgs()

	fmt.Println("Finding CSV files")
	csvList := getCsvListFromDir(args["csvDir"].(string))

	// Create waitgroup so we know when all CSV readers are done
	var csvWg sync.WaitGroup

	// Make sure writer of filenames to this channel never buffers
	csvFilenamesChannel := make(chan string, len(csvList))

	// Create channel to send datapoints to writers
	const (
		// ChannelSize 512 MB of datapoints at 400 bytes/datapoint (roughly)
		ChannelSize int = (512 * 1024 * 1024) / 400
	)
	datapointsChannel := make(chan DriveHealthDatapoint, ChannelSize)

	//fmt.Println("Starting reader workers")
	idCaches := &IdAssignmentCaches{
		DriveModelIdsLock:          sync.RWMutex{},
		DriveIdsLock:               sync.RWMutex{},
		DriveModelsToDriveModelIds: make(map[string]uuid.UUID),
		DriveSerialNumberToId:      make(map[string]uuid.UUID),
	}

	const NumReaders int = 20
	for i := 0; i < NumReaders; i++ {
		csvWg.Add(1)
		go readCsvFiles(idCaches, csvFilenamesChannel, datapointsChannel, &csvWg)
	}

	//fmt.Println("Starting writer workers")
	var datapointWaitgroup sync.WaitGroup
	const NumWriters int = 1
	for i := 0; i < NumWriters; i++ {
		datapointWaitgroup.Add(1)
		go writerWorker(datapointsChannel, &datapointWaitgroup)
	}

	// Send all CSV files over the channel to the readers
	for _, currCsvFilename := range csvList {
		csvFilenamesChannel <- currCsvFilename
	}

	// Close the channel to bring readers home
	close(csvFilenamesChannel)
	//fmt.Println("CSV reads have kicked off")
	// Wait on reader waitgroup
	csvWg.Wait()

	// at this point, we can mark the ID caches eligible for garbage collection
	idCaches = nil

	fmt.Println("All datapoints have been read, waiting for writes to complete")

	// Signal writers that they should terminate once their channel is empty,
	//		as all datapoints are written
	close(datapointsChannel)
	datapointWaitgroup.Wait()

	fmt.Println("All datapoints have been successfully written")
}
