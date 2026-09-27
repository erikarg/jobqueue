DATABASE_URL ?= postgres://jobqueue:jobqueue@localhost:5433/jobqueue?sslmode=disable

.PHONY: db-up db-down run test test-integration lint

db-up:
	docker compose up -d --wait

db-down:
	docker compose down

run:
	DATABASE_URL="$(DATABASE_URL)" go run ./cmd/jobqueue

# Unit tests only (integration tests skip without TEST_DATABASE_URL).
test:
	go test -race ./...

# Unit + integration tests against the compose database.
test-integration:
	TEST_DATABASE_URL="$(DATABASE_URL)" go test -race -count=1 ./...

lint:
	gofmt -l . | (! grep .)
	go vet ./...
