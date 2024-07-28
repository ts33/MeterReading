test:
	go test ./... -v

benchmark:
	go test ./... -v -bench=. -count 3

test-coverage:
	rm -rf .codecov
	mkdir .codecov
	go test ./... -v -coverprofile=./.codecov/cover.out
	go tool cover -html=./.codecov/cover.out

execute:
	go run main.go

pre-commit:
	go mod tidy
	go mod vendor
	go vet
	go fmt ./...

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

generate-sql:
	jet -dsn=postgresql://test123:test123@localhost:5432/postgres?sslmode=disable -schema=public -path=./.gen
