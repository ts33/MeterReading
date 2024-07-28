# Setup
The following needs to be installed
- golang
- docker
- psql
- jet `go install github.com/go-jet/jet/v2/cmd/jet@latest` for sql

Run the command `make pre-commit` to install golang dependencies.

# Tests
## Unit tests
Run the command `make test` to run the unit tests

## Integration Test
- Spin up the docker instance for postgres using the command `make docker-up`
- Run the main go file with the command `make execute`
- Connect to the local postgres instance with the command `psql -h localhost -p 5432 -U test123 -d postgres`
- Validate that records have been created with the query `select * from public.meter_readings`

## Benchmarking
- Run the command `make benchmark` to see benchmark statistics.
- Test files are generated with the script `.scripts/sample_generator.sh`
- The following files are used in the benchmark tests
    - sample_100.csv (100 blocks of NMI 200, 119K size)
    - sample_10000.csv (10000 blocks of NMI 200, 12M size)
    - sample_100000.csv (100000 blocks of NMI 200, 117M size)
- preliminary benchmarks on a 2.50Ghz machine gives
    - sample_100.csv (433323 ns/op)
    - sample_10000.csv (46273497 ns/op)
    - sample_100000.csv (416331417 ns/op)
