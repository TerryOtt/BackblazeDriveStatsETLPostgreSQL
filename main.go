package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type CmdLineArgs struct {
	NumReaders uint
	NumWriters uint
	CsvDir     string
}

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
	datapoint *DriveHealthDatapoint,
	dbHandle *pgx.Conn) {

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

		commandTag, err := dbHandle.Exec(context.Background(),
			"INSERT INTO drive_models (drive_model_id, drive_model, capacity_bytes)"+
				" VALUES ($1, $2, $3);", driveModelId, datapoint.DriveModel, datapoint.CapacityBytes)
		if err != nil {
			panic(err)
		}
		if commandTag.RowsAffected() != 1 {
			panic("Tried to insert new drive model ID but no rows affected")
		}
	}

	// Handle drive serial number cache miss
	if !driveIdOk {
		//fmt.Println("\tCache miss on drive ID")
		driveId = uuid.New()
		caches.DriveSerialNumberToId[datapoint.DriveSerialNumber] = driveId

		commandTag, err := dbHandle.Exec(context.Background(),
			"INSERT INTO drives (drive_id, drive_model, serial_number)"+
				" VALUES ($1, $2, $3);", driveId, driveModelId, datapoint.DriveSerialNumber)
		if err != nil {
			panic(err)
		}
		if commandTag.RowsAffected() != 1 {
			panic("Tried to insert new drive ID but no rows affected")
		}
	}

	// *** CRITICAL SECTION -- END
	caches.CachesLock.Unlock()

	// Populate datapoint
	datapoint.DriveId = driveId
	datapoint.DriveModelId = driveModelId
}

func connectToDB() *pgx.Conn {

	//fmt.Println("\nConnecting to database...")

	pgHost, envVarExists := os.LookupEnv("PGHOST")
	if !envVarExists {
		panic("PGHOST environment variable not set")
	}
	pgUser, envVarExists := os.LookupEnv("PGUSER")
	if !envVarExists {
		panic("PGUSER environment variable not set")
	}
	pgDatabase, envVarExists := os.LookupEnv("PGDATABASE")
	if !envVarExists {
		panic("PGDATABASE environment variable not set")
	}

	// Read .pgpass file so we don't keep password in env vars
	pgPassfile, envVarExists := os.LookupEnv("PGPASSFILE")
	if !envVarExists {
		panic("PGPASSFILE environment variable not set")
	}
	//fmt.Println("Trying to open password file " + pgPassfile)
	pgPassHandle, err := os.Open(pgPassfile)
	if err != nil {
		panic(err)
	}
	defer pgPassHandle.Close()
	pgPassword := ""
	scanner := bufio.NewScanner(pgPassHandle)
	for scanner.Scan() {
		line := scanner.Text()
		tokens := strings.Split(line, ":")
		if len(tokens) != 5 {
			panic("invalid .pgpass file line: " + line)
		}
		// Is this the droids we are looking for?
		if tokens[0] == pgHost {
			// Check DB name match
			if (tokens[2] != "*" && tokens[2] == pgDatabase) || tokens[2] == "*" {
				// Check user
				if (tokens[3] != "*" && tokens[3] == pgUser) || tokens[3] == "*" {
					pgPassword = tokens[4]
					break
				}
			}
		}
	}
	if pgPassword == "" {
		panic("Did not find matching entry in PGPASSFILE file")
	}

	// password is fifth and final token with colon delimiters
	pgConnectDsnString := "host=" + pgHost +
		" user=" + pgUser +
		" password=" + pgPassword +
		" dbname=" + pgDatabase

	//fmt.Println("DB connection string:", pgConnectDsnString)

	dbHandle, err := pgx.Connect(context.Background(), pgConnectDsnString)

	if err != nil {
		panic("Unable to connect to database")
	}
	//fmt.Println("\tSuccessfully connected to database!")

	return dbHandle
}

func readCsvFiles(
	idCaches *IdAssignmentCaches,
	csvChannel chan string,
	datapointsChannel chan DriveHealthDatapoint,
	wg *sync.WaitGroup) {

	dbHandle := connectToDB()
	defer dbHandle.Close(context.Background())

	// Read from channel until it is closed
	for csvFile := range csvChannel {
		fmt.Println("\tReading CSV file ", csvFile)
		fileHandle, err := os.Open(csvFile)
		if err != nil {
			panic(err)
		}
		defer fileHandle.Close()

		// Pull date for this file -- filename minus CSV extension is the date
		fileDate := strings.TrimSuffix(filepath.Base(csvFile), filepath.Ext(csvFile))
		fmt.Println("\tDate of datapoints in CSV:", fileDate)

		// Grab all drive serial numbers seen in the DB on this date, so we can find out which
		//		CSV datapoints are new -- if any
		rows, err := dbHandle.Query(context.Background(),
			"SELECT 	drives.serial_number "+
				"FROM 		drives "+
				"JOIN		drives_dates_seen "+
				"ON 		drives.drive_id = drives_dates_seen.drive_id "+
				"AND 		drives_dates_seen.date_seen = $1;", fileDate)
		if err != nil {
			panic(err)
		}

		// Struct{} is zero bytes, making this map effectively a set
		driveSerialsSeen := make(map[string]struct{})
		var driveSerial string
		_, err = pgx.ForEachRow(rows, []any{&driveSerial}, func() error {
			driveSerialsSeen[driveSerial] = struct{}{}
			return nil
		})
		if err != nil {
			panic("Error when reading drive model info")
		}
		fmt.Println("\tInitialized serials seen on " + fileDate + " with " +
			strconv.Itoa(len(driveSerialsSeen)) + " serials")

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
			// If the DB already has an entry for this serial number on the given date, skip it
			if _, ok := driveSerialsSeen[csvRow[1]]; ok {
				// Skip this datapoint
				continue
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
			addIdValuesToDatapoint(idCaches, &newDatapoint, dbHandle)

			// Now datapoint is safe to pass to datapoint writers for eventual write
			datapointsChannel <- newDatapoint
		}
	}

	// Have to mark all our work as a CSV reader is done before we bail
	wg.Done()
}

func parseArgs() CmdLineArgs {

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

	args := CmdLineArgs{
		uint(*numReaders),
		uint(*numWriters),
		flag.Arg(0),
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

func doDeferredWrites(dbHandle *pgx.Conn, deferredWrites []DriveHealthDatapoint) {
	//fmt.Println("\tDoing deferred writes")

	// Do a COPY command as it's the fastest way to blast data into PostgreSQL
	_, err := dbHandle.CopyFrom(
		context.Background(),
		pgx.Identifier{"drives_dates_seen"},
		[]string{"drive_id", "date_seen", "failure"},
		pgx.CopyFromSlice(len(deferredWrites), func(i int) ([]any, error) {
			return []any{deferredWrites[i].DriveId, deferredWrites[i].Date, deferredWrites[i].Failure}, nil
		}),
	)
	if err != nil {
		panic(err)
	}
	//fmt.Println("\tWrote " + strconv.FormatInt(copyCount, 10) + " datapoints during deferred writes")
}

func writerWorker(args CmdLineArgs, datapointChannel chan DriveHealthDatapoint, wg *sync.WaitGroup) {
	//fmt.Println("Entering writer worker")

	// Want to hold up to 512MB across all writers at ~400 bytes per datapoint
	maxDeferredWrites := ((512 * 1024 * 1024) / 400) / args.NumWriters
	deferredWrites := make([]DriveHealthDatapoint, maxDeferredWrites)
	currDeferredWrites := uint(0)

	dbHandle := connectToDB()
	defer dbHandle.Close(context.Background())

	// Read from channel until it's both empty AND closed
	for currDatapoint := range datapointChannel {
		//fmt.Println("\t\tWriter worker got datapoint: ", currDatapoint)
		if currDatapoint.DriveId == uuid.Nil {
			panic("Writer got a datapoint without a valid drive ID")
		}
		if currDatapoint.DriveModelId == uuid.Nil {
			panic("Writer got a datapoint without a valid drive model ID")
		}

		// queue it
		deferredWrites = append(deferredWrites, currDatapoint)
		currDeferredWrites++

		// Do deferred work if needed
		if currDeferredWrites == maxDeferredWrites {
			doDeferredWrites(dbHandle, deferredWrites)
			deferredWrites = make([]DriveHealthDatapoint, maxDeferredWrites)
			currDeferredWrites = 0
		}
	}

	if currDeferredWrites > 0 {
		doDeferredWrites(dbHandle, deferredWrites)
	}

	// Mark that we've done our work to let the waitgroup proceed towards unblocking
	wg.Done()
}

func createAndPopulateCaches() *IdAssignmentCaches {
	fmt.Println("\nCreating and populating ID caches...")
	idCaches := &IdAssignmentCaches{
		CachesLock:                 sync.RWMutex{},
		DriveModelsToDriveModelIds: make(map[string]uuid.UUID),
		DriveSerialNumberToId:      make(map[string]uuid.UUID),
	}

	dbHandle := connectToDB()
	defer dbHandle.Close(context.Background())

	rows, _ := dbHandle.Query(context.Background(),
		"SELECT 	drive_model, drive_model_id "+
			"FROM 		drive_models;")

	var modelString string
	var modelId uuid.UUID
	_, err := pgx.ForEachRow(rows, []any{&modelString, &modelId}, func() error {
		idCaches.DriveModelsToDriveModelIds[modelString] = modelId
		return nil
	})
	if err != nil {
		panic("Error when reading drive model info")
	}

	rows, _ = dbHandle.Query(context.Background(),
		"SELECT 	serial_number, drive_id "+
			"FROM 		drives;")

	var serialNumber string
	var driveID uuid.UUID
	_, err = pgx.ForEachRow(rows, []any{&serialNumber, &driveID}, func() error {
		idCaches.DriveSerialNumberToId[serialNumber] = driveID
		return nil
	})
	if err != nil {
		panic("Error when reading drive infos")
	}

	fmt.Println("\tNumber of drive models in DB: " + strconv.Itoa(len(idCaches.DriveModelsToDriveModelIds)))
	fmt.Println("\t      Number of drives in DB: " + strconv.Itoa(len(idCaches.DriveSerialNumberToId)))
	return idCaches
}

func main() {
	//fmt.Println("Parsing cmdline args")
	args := parseArgs()

	fmt.Println("\nFinding CSV files...")
	csvList := getCsvListFromDir(args.CsvDir)

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

	idCaches := createAndPopulateCaches()

	//fmt.Println("Starting reader workers")
	NumReaders := min(uint(len(csvList)), args.NumReaders)
	for i := uint(0); i < NumReaders; i++ {
		csvWg.Add(1)
		go readCsvFiles(idCaches, csvFilenamesChannel, datapointsChannel, &csvWg)
	}
	//fmt.Println("Started reader worker pool with size: " + strconv.FormatUint(uint64(NumReaders), 10))

	//fmt.Println("Starting writer workers")
	var datapointWaitgroup sync.WaitGroup
	NumWriters := args.NumWriters
	for i := uint(0); i < NumWriters; i++ {
		datapointWaitgroup.Add(1)
		go writerWorker(args, datapointsChannel, &datapointWaitgroup)
	}
	//fmt.Println("Started writer worker pool with size: " + strconv.FormatUint(uint64(NumWriters), 10))

	fmt.Println("\nStarting data processing pipeline...")

	// Send all CSV files over the channel to the readers
	for _, currCsvFilename := range csvList {
		csvFilenamesChannel <- currCsvFilename
	}
	fmt.Println("\tAll CSV filenames sent to reader worker pool, waiting for reader workers to complete")

	// Close the channel to bring readers home
	close(csvFilenamesChannel)
	//fmt.Println("CSV reads have kicked off")
	// Wait on reader waitgroup
	csvWg.Wait()

	// at this point, we can mark the ID caches eligible for garbage collection
	idCaches = nil

	fmt.Println("\tReader workers have processed all CSV files, waiting for writes to complete")

	// Signal writers that they should terminate once their channel is empty,
	//		as all datapoints are written
	close(datapointsChannel)
	datapointWaitgroup.Wait()
	fmt.Println("\tAll new datapoints from CSV have been successfully written")

	fmt.Println("\nDone!")
}
