package main

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	sql "database/sql"
	_ "github.com/lib/pq"
	model "github.com/ts33/energy-reading/.gen/postgres/public/model"
	repo "github.com/ts33/energy-reading/repository"
)

const (
	RecordIndicator_100      = "100"
	RecordIndicator_200      = "200"
	RecordIndicator_300      = "300"
	RecordIndicator_500      = "500"
	RecordIndicator_900      = "900"
	RecordTimestampLayout    = "20060102"
	MeterReadingDecimalPlace = 3

	dbHost     = "localhost"
	dbPort     = 5432
	dbUser     = "test123"
	dbPassword = "test123"
	dbName     = "postgres"
)

// MeterReadingFactor is the number of decimal places a MeterReading should be restricted to.
var MeterReadingFactor = math.Pow(10, float64(MeterReadingDecimalPlace))

// NmiWorkerParams contains a slice of NMI 300 records that belong to a NMI 200 block.
type NmiWorkerParams struct {
	NmiBlockRecords []string
	Nmi             string
}

// NmiResultsParams contains a slice of MeterReadings that are ready to be inserted into the datastore.
type NmiResultsParams struct {
	MeterReadings []*model.MeterReadings
}

func main() {
	// 1. setup db
	var connectString = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", dbHost, dbPort, dbUser, dbPassword, dbName)
	db, err := sql.Open("postgres", connectString)
	// to be handled by caller
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 2. Process NMI File
	readings, _, err := ProcessNmiFile("test_files/sample.csv", 1)
	// NMI failures can be handled for reruns
	if err != nil {
		panic(err)
	}

	// 3. write to DB
	err = repo.BulkInsertMeterReadings(db, readings)
	// to be handled by caller
	if err != nil {
		panic(err)
	}
}

// ProcessNmiFile reads an NMI file, processes it and saves the records into a datastore.
func ProcessNmiFile(fileName string, numWorkers int) (allReadings []*model.MeterReadings, failedNmis []string, err error) {
	allReadings = []*model.MeterReadings{}
	failedNmis = []string{}

	// 1. Open the file
	file, err := os.Open(fileName)
	if err != nil {
		return allReadings, failedNmis, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)

	// 2. Check that file starts with 100
	valid := scanner.Scan()
	if !valid {
		return allReadings, failedNmis, errors.New("unable to read first line")
	}
	line := scanner.Text()
	if line[:3] != RecordIndicator_100 {
		return allReadings, failedNmis, errors.New("first record is not a 100 record")
	}

	// 3.1 Create channels for work distribution - round workers to nearest multiple of 2
	// good reference: https://stackoverflow.com/a/50261948/471538
	jobsChan := make(chan NmiWorkerParams, numWorkers)
	resultsChan := make(chan NmiResultsParams, numWorkers)
	failedChan := make(chan string, numWorkers)
	var wgWorker, wgOutput sync.WaitGroup
	var muResults, muFailed sync.Mutex

	// 3.2 Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wgWorker.Add(1)
		go NmiBlockWorker(jobsChan, &wgWorker, resultsChan, failedChan)
	}
	wgOutput.Add(2)
	// 3.3 Start goroutine that reads from results
	go func() {
		defer wgOutput.Done()
		for result := range resultsChan {
			muResults.Lock()
			allReadings = append(allReadings, result.MeterReadings...)
			muResults.Unlock()
		}
	}()
	// 3.4 Start goroutine that reads from failedChan
	go func() {
		defer wgOutput.Done()
		for failedNmi := range failedChan {
			muFailed.Lock()
			failedNmis = append(failedNmis, failedNmi)
			muFailed.Unlock()
		}
	}()

	// 4. loop through file and process nmiBlocks
	var nmiBlockRecords []string
	var nem string

	for scanner.Scan() {
		line = scanner.Text()
		indicator := line[:3]

		switch indicator {
		case RecordIndicator_200:
			// process the previous batch if available
			if len(nmiBlockRecords) > 0 {
				jobsChan <- NmiWorkerParams{nmiBlockRecords, nem}
				// reset blocks
				nmiBlockRecords = []string{}
			}
			// capture the new NEM value
			splitLine := strings.Split(line, ",")
			nem = splitLine[1]
		case RecordIndicator_300:
			nmiBlockRecords = append(nmiBlockRecords, line)
		case RecordIndicator_900:
			// process the last batch
			if len(nmiBlockRecords) > 0 {
				jobsChan <- NmiWorkerParams{nmiBlockRecords, nem}
			}
		default:
			continue
		}
	}

	// 5. Validate end of file indicator
	if line[:3] != RecordIndicator_900 {
		return allReadings, failedNmis, errors.New("last record is not a 900 record")
	}

	// 6.1 Explicitly close jobs channels as file reading is complete
	// good reference: https://stackoverflow.com/a/59639259/471538
	close(jobsChan)
	// 6.2 Wait for all workers to finish
	wgWorker.Wait()
	// 6.3 close results and errors channel as all workers are done
	close(resultsChan)
	close(failedChan)
	// 6.4 Wait for the two output go routines to finish
	wgOutput.Wait()

	return allReadings, failedNmis, nil
}

// NmiBlockWorker is a worker that receives nmiBlocks, processes them and sends the output to the results channel.
func NmiBlockWorker(jobsChan <-chan NmiWorkerParams, wg *sync.WaitGroup, resultsChan chan<- NmiResultsParams, failedChan chan<- string) {
	defer wg.Done()
	for j := range jobsChan {
		readings, err := ProcessNmiBlock(j.NmiBlockRecords, j.Nmi)
		// push err to error chan if it exists, for reconciliation
		if err != nil {
			failedChan <- j.Nmi
		} else {
			resultsChan <- NmiResultsParams{readings}
		}
	}
}

// ProcessNmiBlock creates a MeterReadings model object for each nmiBlockRecord received.
func ProcessNmiBlock(nmiBlockRecords []string, nmi string) (meterReadings []*model.MeterReadings, err error) {
	meterReadings = []*model.MeterReadings{}

	for _, nmiBlockRecord := range nmiBlockRecords {
		splitLine := strings.Split(nmiBlockRecord, ",")
		if len(splitLine) < 51 {
			return meterReadings, errors.New("meter reading does not have enough values")
		}
		timestamp, err := time.Parse(RecordTimestampLayout, splitLine[1])
		if err != nil {
			return meterReadings, fmt.Errorf("%s: %w", "Failed to parse time value", err)
		}
		sum, err := sumConsumptionValues(splitLine[2:50])
		if err != nil {
			return meterReadings, fmt.Errorf("%s: %w", "Failed to parse consumption value to float", err)
		}

		meterReading := &model.MeterReadings{
			Nmi:         nmi,
			Timestamp:   timestamp,
			Consumption: sum,
		}
		meterReadings = append(meterReadings, meterReading)
	}
	return meterReadings, nil
}

// sumConsumptionValues takes in a list of stringified floats and sums them up.
// It also forces the floats and the end value to be restricted to only a set amount of decimal places based on MeterReadingFactor.
func sumConsumptionValues(numbers []string) (sum float64, err error) {
	sum = 0.0
	for _, num := range numbers {
		val, err := strconv.ParseFloat(num, 64)
		if err != nil {
			return 0.0, err
		}
		val = math.Round(val*MeterReadingFactor) / MeterReadingFactor
		sum += val
	}
	// this can be rounded up or down depending on requirements
	return math.Round(sum*MeterReadingFactor) / MeterReadingFactor, nil
}
