SHELL := "C:\Windows\system32\bash.exe"
.SHELLFLAGS := -ec

.PHONY: help build test lint fmt vet migrate vuln sec run-worker run-producer run-scheduler run-deadletter docker-up docker-down docker-logs clean docker-build-worker docker-build-scheduler docker-build-producer docker-build-deadletter

build:
	go build -o bin/producer.exe ./cmd/producer
	go build -o bin/worker.exe ./cmd/worker
	go build -o bin/scheduler.exe ./cmd/scheduler
	go build -o bin/deadletter.exe ./cmd/deadletter

test:
	go test -v ./...

lint:
	golangci-lint run ./...

fmt:
	goimports -w .

vet:
	go vet ./...

migrate:
	docker cp migrations/0003_multi-tenancy.sql kairos-db:/0003_multi-tenancy.sql
	docker exec -it kairos-db psql -U kairos -d kairos -f /0003_multi-tenancy.sql

run-worker:
	go run ./cmd/worker

run-producer:
	go run ./cmd/producer

run-scheduler:
	go run ./cmd/scheduler

run-deadletter:
	go run ./cmd/deadletter $(ARGS)

run-cron:
	go run ./cmd/cron

seed-recurring:
	go run ./cmd/seed-recurring

clean:
	rm -rf bin/

vuln:
	govulncheck ./...

sec:
	gosec -exclude-generated ./...

docker-build-worker:
	docker build --build-arg CMD_PATH=cmd/worker -t kairos-worker .

docker-build-scheduler:
	docker build --build-arg CMD_PATH=cmd/scheduler -t kairos-scheduler .

docker-build-producer:
	docker build --build-arg CMD_PATH=cmd/producer -t kairos-producer .

docker-build-deadletter:
	docker build --build-arg CMD_PATH=cmd/deadletter -t kairos-deadletter .

grafana: ## Open Grafana in the browser (admin/kairos, or anonymous viewer access)
	@echo "Grafana: http://localhost:3000"
	@echo "Prometheus: http://localhost:9090"

bench: ## Run all benchmarks
	go test -bench=. -benchmem ./...

bench-save: ## Run benchmarks and save output for comparison (see `make bench-compare`)
	go test -bench=. -benchmem ./... > bench_baseline.txt

bench-compare: ## Compare current benchmarks against the saved baseline
	go test -bench=. -benchmem ./... > bench_current.txt
	go install golang.org/x/perf/cmd/benchstat@latest
	benchstat bench_baseline.txt bench_current.txt

run-examples: ## Run all examples in sequence (requires make docker-up first)
	"C:/Program Files/Git/bin/bash.exe" -c 'for dir in examples/*/; do if [ -f "$$dir/main.go" ]; then echo "=== $$dir ==="; go run ./$$dir; echo ""; fi; done'

loadtest-enqueue:
	k6 run loadtest/enqueue.js

loadtest-deadletter:
	k6 run loadtest/deadletter-read.js

fuzz-job:
	go test -fuzz=FuzzNew_NeverPanicsOnPayload -fuzztime=30s ./internal/job/...

fuzz-queue:
	go test -fuzz=FuzzJobMarshalUnmarshalRoundTrip -fuzztime=30s ./internal/queue/...

arch-lint:
	go-arch-lint check
