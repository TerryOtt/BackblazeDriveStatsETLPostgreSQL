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
	CachesLock                 sync.RWMutex
	DriveModelsToDriveModelIds map[string]uuid.UUID
	DriveSerialNumberToId      map[string]uuid.UUID
}

func addIdValuesToDatapoint(
	caches *IdAssignmentCaches,
	datapoint *DriveHealthDatapoint) {

	caches.CachesLock.RLock()
	// *** CRITICAL SECTION -- READ ONLY -- BEGIN
	driveId, driveIdOk := caches.DriveSerialNumberToId[datapoint.DriveSerialNumber]
	driveModelId, driveModelIdOk := caches.DriveModelsToDriveModelIds[datapoint.DriveModel]
	// *** CRITICAL SECTION -- READ ONLY -- END
	caches.CachesLock.RUnlock()

	if driveIdOk {
		// We knew the drive ID, we better know everything
		if !driveModelIdOk {
			panic("Knew drive ID but not model ID, should not happen")
		}
		//fmt.Println("\tCache hit!")
		datapoint.DriveId = driveId
		datapoint.DriveModelId = driveModelId
		return
	}

	// Acquire exclusive write lock and make any insertions to BOTH cache and Postgres that are needed
	caches.CachesLock.Lock()

	// *** CRITICAL SECTION -- BEGIN

	// Need to refresh reads as we gave up the read lock, another goroutine could have updated
	//	one or both values before we acquired the write lock
	driveId, driveIdOk = caches.DriveSerialNumberToId[datapoint.DriveSerialNumber]
	driveModelId, driveModelIdOk = caches.DriveModelsToDriveModelIds[datapoint.DriveModel]

	// Handle drive model cache miss
	if !driveModelIdOk {
		//fmt.Println("\tCache miss on drive model ID")
		driveModelId = uuid.New()
		caches.DriveModelsToDriveModelIds[datapoint.DriveModel] = driveModelId
		// TODO: add to postgres
	}

	// Handle drive serial number cache miss
	if !driveIdOk {
		//fmt.Println("\tCache miss on drive ID")
		driveId = uuid.New()
		caches.DriveSerialNumberToId[datapoint.DriveSerialNumber] = driveId
		// TODO: add to postgres
	}

	// *** CRITICAL SECTION -- END
	caches.CachesLock.Unlock()

	// Populate datapoint
	datapoint.DriveId = driveId
	datapoint.DriveModelId = driveModelId
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

			// This call adds any new ID's generated to postgres before returning
			addIdValuesToDatapoint(idCaches, &newDatapoint)

			// Now datapoint is safe to pass to datapoint writers for eventual write
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
	for currDatapoint := range datapointChannel {
		//fmt.Println("\t\tWriter worker got datapoint: ", currDatapoint)
		if currDatapoint.DriveId == uuid.Nil {
			panic("Writer got a datapoint without a valid drive ID")
		}
		if currDatapoint.DriveModelId == uuid.Nil {
			panic("Writer got a datapoint without a valid drive model ID")
		}
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
		CachesLock:                 sync.RWMutex{},
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
