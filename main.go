package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"
)

type Options struct {
	InputFilePath string
	OutputDir     string
	Timeout       int
}

func main() {
	opts := Options{
		OutputDir: ".",
		Timeout:   5000,
	}

	flag.StringVar(&opts.InputFilePath, "file", "", "input file path")

	flag.Parse()

	if opts.InputFilePath == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Duration(opts.Timeout)*time.Millisecond))
	defer cancel()

	if err := pipeline(ctx, &opts); err != nil {
		log.Fatal(err)
	}
}
