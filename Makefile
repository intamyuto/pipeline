build: lint
	CGO_ENABLED=0 go build -o pipeline

lint:
	go get github.com/golangci/golangci-lint/cmd/golangci-lint
	golangci-lint run

FILE = trades.csv

run:
	go run -race main.go pipeline.go --file ${FILE}