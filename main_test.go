package main_test

import (
	"errors"
	energ "github.com/ts33/energy-reading"
	model "github.com/ts33/energy-reading/.gen/postgres/public/model"
	"reflect"
	"sort"
	"testing"
	"time"
)

// benchmark with 100 records and 5 workers per pool
func BenchmarkProcessNmiFile100(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_,_,_ = energ.ProcessNmiFile("test_files/sample_100.csv", 5)
	}
}

// benchmark with 10000 records and 5 workers per pool
func BenchmarkProcessNmiFile10000(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_,_,_ = energ.ProcessNmiFile("test_files/sample_10000.csv", 5)
	}
}

// benchmark with 100000 records and 5 workers per pool
func BenchmarkProcessNmiFile100000(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_,_,_ = energ.ProcessNmiFile("test_files/sample_100000.csv", 5)
	}
}

type ProcessNmiFileTestCase struct {
	Name                       string
	ProcessNmiFileTestInput    ProcessNmiFileTestInput
	ProcessNmiFileTestExpected ProcessNmiFileTestExpected
}

type ProcessNmiFileTestInput struct {
	fileName   string
	numWorkers int
}

type ProcessNmiFileTestExpected struct {
	MeterReadings []*model.MeterReadings
	FailedNmis    []string
	Err           error
}

func TestProcessNmiFile(t *testing.T) {
	tests := []ProcessNmiFileTestCase{
		{
			Name: "Happy Case - Process File sample",
			ProcessNmiFileTestInput: ProcessNmiFileTestInput{
				fileName:   "test_files/sample.csv",
				numWorkers: 1,
			},
			ProcessNmiFileTestExpected: ProcessNmiFileTestExpected{
				MeterReadings: []*model.MeterReadings{
					{
						Nmi:         "NEM1201009",
						Timestamp:   time.Date(2005, time.March, 1, 0, 0, 0, 0, time.UTC),
						Consumption: 31.444,
					},
					{
						Nmi:         "NEM1201009",
						Timestamp:   time.Date(2005, time.March, 2, 0, 0, 0, 0, time.UTC),
						Consumption: 32.24,
					},
					{
						Nmi:         "NEM1201009",
						Timestamp:   time.Date(2005, time.March, 3, 0, 0, 0, 0, time.UTC),
						Consumption: 29.789,
					},
					{
						Nmi:         "NEM1201009",
						Timestamp:   time.Date(2005, time.March, 4, 0, 0, 0, 0, time.UTC),
						Consumption: 34.206,
					},
					{
						Nmi:         "NEM1201010",
						Timestamp:   time.Date(2005, time.March, 1, 0, 0, 0, 0, time.UTC),
						Consumption: 33.19,
					},
					{
						Nmi:         "NEM1201010",
						Timestamp:   time.Date(2005, time.March, 2, 0, 0, 0, 0, time.UTC),
						Consumption: 31.811,
					},
					{
						Nmi:         "NEM1201010",
						Timestamp:   time.Date(2005, time.March, 3, 0, 0, 0, 0, time.UTC),
						Consumption: 34.204,
					},
					{
						Nmi:         "NEM1201010",
						Timestamp:   time.Date(2005, time.March, 4, 0, 0, 0, 0, time.UTC),
						Consumption: 31.354,
					},
				},
				FailedNmis: []string{},
				Err:        nil,
			},
		},
		{
			Name: "Error Case - file does not exist",
			ProcessNmiFileTestInput: ProcessNmiFileTestInput{
				fileName:   "test_files/does_not_exist.csv",
				numWorkers: 1,
			},
			ProcessNmiFileTestExpected: ProcessNmiFileTestExpected{
				MeterReadings: []*model.MeterReadings{},
				FailedNmis:    []string{},
				Err:           errors.New("open test_files/does_not_exist.csv: no such file or directory"),
			},
		},
		{
			Name: "Error Case - empty file",
			ProcessNmiFileTestInput: ProcessNmiFileTestInput{
				fileName:   "test_files/sample_empty.csv",
				numWorkers: 1,
			},
			ProcessNmiFileTestExpected: ProcessNmiFileTestExpected{
				MeterReadings: []*model.MeterReadings{},
				FailedNmis:    []string{},
				Err:           errors.New("unable to read first line"),
			},
		},
		{
			Name: "Error Case - first record not 100",
			ProcessNmiFileTestInput: ProcessNmiFileTestInput{
				fileName:   "test_files/sample_err_no_100.csv",
				numWorkers: 1,
			},
			ProcessNmiFileTestExpected: ProcessNmiFileTestExpected{
				MeterReadings: []*model.MeterReadings{},
				FailedNmis:    []string{},
				Err:           errors.New("first record is not a 100 record"),
			},
		},
		{
			Name: "Error Case - last record not 900",
			ProcessNmiFileTestInput: ProcessNmiFileTestInput{
				fileName:   "test_files/sample_err_no_900.csv",
				numWorkers: 1,
			},
			ProcessNmiFileTestExpected: ProcessNmiFileTestExpected{
				MeterReadings: []*model.MeterReadings{},
				FailedNmis:    []string{},
				Err:           errors.New("last record is not a 900 record"),
			},
		},
		{
			Name: "Happy Case - partial processing",
			ProcessNmiFileTestInput: ProcessNmiFileTestInput{
				fileName:   "test_files/sample_err_partial.csv",
				numWorkers: 2,
			},
			ProcessNmiFileTestExpected: ProcessNmiFileTestExpected{
				MeterReadings: []*model.MeterReadings{
					{
						Nmi:         "NEM1201009",
						Timestamp:   time.Date(2005, time.March, 1, 0, 0, 0, 0, time.UTC),
						Consumption: 31.444,
					},
					{
						Nmi:         "NEM1201009",
						Timestamp:   time.Date(2005, time.March, 2, 0, 0, 0, 0, time.UTC),
						Consumption: 32.24,
					},
					{
						Nmi:         "NEM1201009",
						Timestamp:   time.Date(2005, time.March, 3, 0, 0, 0, 0, time.UTC),
						Consumption: 29.789,
					},
					{
						Nmi:         "NEM1201009",
						Timestamp:   time.Date(2005, time.March, 4, 0, 0, 0, 0, time.UTC),
						Consumption: 34.206,
					},
				},
				FailedNmis: []string{
					"NEM1201010",
					"NEM1201011",
					"NEM1201012",
				},
				Err: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			result, failedNmis, err := energ.ProcessNmiFile(
				tt.ProcessNmiFileTestInput.fileName,
				tt.ProcessNmiFileTestInput.numWorkers,
			)

			// assert that number of failed NMI blocks are equal
			if len(failedNmis) != len(tt.ProcessNmiFileTestExpected.FailedNmis) {
				t.Errorf("Expected %+v number of failed NMIs, got %+v number of failed NMIs instead", len(tt.ProcessNmiFileTestExpected.FailedNmis), len(failedNmis))
			}
			sort.Slice(failedNmis, func(i, j int) bool {
				return failedNmis[i] < failedNmis[j]
			})
			if len(failedNmis) > 0 {
				for i, failed := range failedNmis {
					if failed != tt.ProcessNmiFileTestExpected.FailedNmis[i] {
						t.Errorf("Expected failed NMI %v, got %v instead", tt.ProcessNmiFileTestExpected.FailedNmis[i], failed)
					}
				}
			}

			// assert that errors are raised
			if err != nil {
				if tt.ProcessNmiFileTestExpected.Err == nil {
					t.Errorf("Expected no error, got %v instead", err)
				}
				if tt.ProcessNmiFileTestExpected.Err.Error() != err.Error() {
					t.Errorf("Expected err %v, got %v instead", tt.ProcessNmiFileTestExpected.Err, err)
				}
			}

			// assert result is correct
			if len(result) != len(tt.ProcessNmiFileTestExpected.MeterReadings) {
				t.Errorf("Expected %+v number of readings, got %+v number of readings instead", len(tt.ProcessNmiFileTestExpected.MeterReadings), len(result))
			}
			// sort the results so that we can compare it with the expected output
			sort.Slice(result, func(i, j int) bool {
				if result[i].Nmi == result[j].Nmi {
					return result[i].Timestamp.Before(result[j].Timestamp)
				}
				return result[i].Nmi < result[j].Nmi
			})
			for i, meterReading := range result {
				if reflect.DeepEqual(tt.ProcessNmiFileTestExpected.MeterReadings[i], meterReading) != true {
					t.Errorf("Expected %+v, got %+v instead", tt.ProcessNmiFileTestExpected.MeterReadings[i], meterReading)
				}
			}
		})
	}
}

type ProcessNmiBlockTestCase struct {
	Name                        string
	ProcessNmiBlockTestInput    ProcessNmiBlockTestInput
	ProcessNmiBlockTestExpected ProcessNmiBlockTestExpected
}

type ProcessNmiBlockTestInput struct {
	NmiBlockRecords []string
	Nmi             string
}

type ProcessNmiBlockTestExpected struct {
	MeterReadings []*model.MeterReadings
	Err           error
}

func TestProcessNmiBlock(t *testing.T) {
	tests := []ProcessNmiBlockTestCase{
		{
			Name: "Happy Case - Process Blocks",
			ProcessNmiBlockTestInput: ProcessNmiBlockTestInput{
				NmiBlockRecords: []string{
					"300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204",
					"300,20050302,0,0,0,0,0,0,0,0,0,0,0,0,0.235,0.567,0.890,1.123,1.345,1.567,1.543,1.234,0.987,1.123,0.876,1.345,1.145,1.173,1.265,0.987,0.678,0.998,0.768,0.954,0.876,0.845,0.932,0.786,0.999,0.879,0.777,0.578,0.709,0.772,0.625,0.653,0.543,0.599,0.432,0.432,A,,,20050310121004,20050310182204",
					"300,20050303,0,0,0,0,0,0,0,0,0,0,0,0,0.261,0.310,0.678,0.934,1.211,1.134,1.423,1.370,0.988,1.207,0.890,1.320,1.130,1.913,1.180,0.950,0.746,0.635,0.956,0.887,0.560,0.700,0.788,0.668,0.543,0.738,0.802,0.490,0.598,0.809,0.520,0.670,0.570,0.600,0.289,0.321,A,,,20050310121004,20050310182204",
					"300,20050304,0,0,0,0,0,0,0,0,0,0,0,0,0.335,0.667,0.790,1.023,1.145,1.777,1.563,1.344,1.087,1.453,0.996,1.125,1.435,1.263,1.085,1.487,1.278,0.768,0.878,0.754,0.476,1.045,1.132,0.896,0.879,0.679,0.887,0.784,0.954,0.712,0.599,0.593,0.674,0.799,0.232,0.612,A,,,20050310121004,20050310182204",
				},
				Nmi: "NEM1201009",
			},
			ProcessNmiBlockTestExpected: ProcessNmiBlockTestExpected{
				MeterReadings: []*model.MeterReadings{
					{
						Nmi:         "NEM1201009",
						Timestamp:   time.Date(2005, time.March, 1, 0, 0, 0, 0, time.UTC),
						Consumption: 31.444,
					},
					{
						Nmi:         "NEM1201009",
						Timestamp:   time.Date(2005, time.March, 2, 0, 0, 0, 0, time.UTC),
						Consumption: 32.24,
					},
					{
						Nmi:         "NEM1201009",
						Timestamp:   time.Date(2005, time.March, 3, 0, 0, 0, 0, time.UTC),
						Consumption: 29.789,
					},
					{
						Nmi:         "NEM1201009",
						Timestamp:   time.Date(2005, time.March, 4, 0, 0, 0, 0, time.UTC),
						Consumption: 34.206,
					},
				},
				Err: nil,
			},
		},
		{
			Name: "Happy Case - Empty Block",
			ProcessNmiBlockTestInput: ProcessNmiBlockTestInput{
				NmiBlockRecords: []string{},
				Nmi:             "NEM1201009",
			},
			ProcessNmiBlockTestExpected: ProcessNmiBlockTestExpected{
				MeterReadings: []*model.MeterReadings{},
				Err:           nil,
			},
		},
		{
			Name: "Error Case - Meter Record Incomplete",
			ProcessNmiBlockTestInput: ProcessNmiBlockTestInput{
				NmiBlockRecords: []string{
					"300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345",
				},
				Nmi: "NEM1201009",
			},
			ProcessNmiBlockTestExpected: ProcessNmiBlockTestExpected{
				MeterReadings: []*model.MeterReadings{},
				Err:           errors.New("meter reading does not have enough values"),
			},
		},
		{
			Name: "Error Case - Timestamp wrong format",
			ProcessNmiBlockTestInput: ProcessNmiBlockTestInput{
				NmiBlockRecords: []string{
					"300,2005030101,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204",
				},
				Nmi: "NEM1201009",
			},
			ProcessNmiBlockTestExpected: ProcessNmiBlockTestExpected{
				MeterReadings: []*model.MeterReadings{},
				Err:           errors.New("Failed to parse time value: parsing time \"2005030101\": extra text: \"01\""),
			},
		},
		{
			Name: "Error Case - Consumption Block Values Wrong",
			ProcessNmiBlockTestInput: ProcessNmiBlockTestInput{
				NmiBlockRecords: []string{
					"300,20050301,abc,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204",
				},
				Nmi: "NEM1201009",
			},
			ProcessNmiBlockTestExpected: ProcessNmiBlockTestExpected{
				MeterReadings: []*model.MeterReadings{},
				Err:           errors.New("Failed to parse consumption value to float: strconv.ParseFloat: parsing \"abc\": invalid syntax"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			result, err := energ.ProcessNmiBlock(
				tt.ProcessNmiBlockTestInput.NmiBlockRecords,
				tt.ProcessNmiBlockTestInput.Nmi,
			)

			// assert that errors are raised
			if err != nil {
				if tt.ProcessNmiBlockTestExpected.Err == nil {
					t.Errorf("Expected no error, got %v instead", err)
				}
				if tt.ProcessNmiBlockTestExpected.Err.Error() != err.Error() {
					t.Errorf("Expected err %v, got %v instead", tt.ProcessNmiBlockTestExpected.Err, err)
				}
			}

			// assert result is correct
			if len(result) != len(tt.ProcessNmiBlockTestExpected.MeterReadings) {
				t.Errorf("Expected %+v number of readings, got %+v number of readings instead", len(tt.ProcessNmiBlockTestExpected.MeterReadings), len(result))
			}
			for i, meterReading := range result {
				if reflect.DeepEqual(tt.ProcessNmiBlockTestExpected.MeterReadings[i], meterReading) != true {
					t.Errorf("Expected %+v, got %+v instead", tt.ProcessNmiBlockTestExpected.MeterReadings[i], meterReading)
				}
			}
		})
	}
}
