package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	ErrInvalidLineFormat = errors.New("parse: invalid line format")
)

type Line struct {
	Ticker    string
	Timestamp time.Time
	Price     int
	Count     int
}

type Candle struct {
	Ticker     string
	Start      time.Time
	PriceStart int
	PriceEnd   int
	PriceMin   int
	PriceMax   int
}

func newCandle(ts time.Time, ticker string, price int) *Candle {
	return &Candle{
		Ticker:     ticker,
		Start:      ts,
		PriceStart: price,
		PriceMax:   price,
		PriceMin:   price,
		PriceEnd:   price,
	}
}

func pipeline(ctx context.Context, opts *Options) error {
	in, err := os.Open(opts.InputFilePath)
	if err != nil {
		return err
	}
	defer in.Close()

	errch := make(chan error)
	done := make(chan struct{})

	tasks := []struct {
		span time.Duration
		in   chan *Line
		out  chan *Candle
	}{
		{span: 5 * time.Minute, in: make(chan *Line), out: make(chan *Candle)},
		{span: 30 * time.Minute, in: make(chan *Line), out: make(chan *Candle)},
		{span: 240 * time.Minute, in: make(chan *Line), out: make(chan *Candle)},
	}

	ins := make([]chan<- *Line, 0, len(tasks))
	for _, task := range tasks {
		ins = append(ins, task.in)

		path := filepath.Join(opts.OutputDir, fmt.Sprintf("candles_%.fmin.csv", task.span.Minutes()))
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
		defer f.Close()

		go process(task.span, task.in, task.out)
		go write(f, errch, task.out, done)
	}
	go read(in, errch, ins...)

	for i := 0; i < len(tasks); i++ {
		select {
		case err := <-errch:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			// spin
		}
	}
	return nil
}

func read(r io.Reader, errch chan<- error, outs ...chan<- *Line) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		text := scanner.Text()
		line, err := parse(text)
		if err != nil {
			errch <- err
		}

		for _, out := range outs {
			out <- line
		}
	}
	if err := scanner.Err(); err != nil {
		errch <- err
	}

	for _, out := range outs {
		close(out)
	}
}

const (
	TradesStart    = 420 * time.Minute  // 07:00 UTC
	TradesDuration = 1020 * time.Minute // 24:00 UTC
)

func process(span time.Duration, in <-chan *Line, out chan<- *Candle) {
	candles := make(map[string]*Candle)

	line, ok := <-in
	if !ok {
		close(out)
		return
	}

	y, m, d := line.Timestamp.Date()
	day := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	start := day.Add(TradesStart)

	interval := struct {
		l, r, max time.Time
	}{
		l:   start,
		r:   start.Add(span),
		max: start.Add(TradesDuration),
	}

	if !line.Timestamp.Before(interval.l) {
		candles[line.Ticker] = newCandle(interval.l, line.Ticker, line.Price)
	}

	for line := range in {
		if line.Timestamp.Before(interval.l) {
			continue // discard line
		}

		if line.Timestamp.After(interval.r) || line.Timestamp.Equal(interval.r) {
			// flush candles
			for k, c := range candles {
				out <- c
				delete(candles, k)
			}

			// adjust interval
			for line.Timestamp.After(interval.r) {
				if interval.r.Equal(interval.max) {
					day = day.Add(24 * time.Hour)
					interval.l = day.Add(TradesStart)
					interval.r = interval.l.Add(span)
					interval.max = interval.l.Add(TradesDuration)
				} else {
					interval.l = interval.l.Add(span)
					interval.r = interval.r.Add(span)
					if interval.r.After(interval.max) {
						interval.r = interval.max
					}
				}
			}

			// proceed with aggregation
		}

		// aggregate lines by ticker
		if candle, ok := candles[line.Ticker]; ok {
			if line.Price < candle.PriceMin {
				candle.PriceMin = line.Price
			}
			if line.Price > candle.PriceMax {
				candle.PriceMax = line.Price
			}
			candle.PriceEnd = line.Price
		} else {
			candles[line.Ticker] = newCandle(interval.l, line.Ticker, line.Price)
		}
	}

	// send remaining candles
	for _, candle := range candles {
		out <- candle
	}

	close(out)
}

func write(w io.Writer, errch chan<- error, in <-chan *Candle, out chan<- struct{}) {
	for candle := range in {
		if _, err := fmt.Fprintf(w, "%s,%s,%s,%s,%s,%s\n",
			candle.Ticker,
			candle.Start.Format(time.RFC3339),
			formatPrice(candle.PriceStart),
			formatPrice(candle.PriceMax),
			formatPrice(candle.PriceMin),
			formatPrice(candle.PriceEnd),
		); err != nil {
			errch <- err
		}
	}

	out <- struct{}{}
}

func parse(str string) (*Line, error) {
	line := new(Line)

	parts := strings.Split(str, ",")
	if len(parts) < 4 {
		return nil, ErrInvalidLineFormat
	}

	line.Ticker = parts[0]

	price, err := parsePrice(parts[1])
	if err != nil {
		return nil, err
	}
	line.Price = price

	i, err := strconv.ParseInt(parts[2], 10, 0)
	if err != nil {
		return nil, err
	}
	line.Count = int(i)

	ts, err := time.Parse("2006-01-02 15:04:05.999999", parts[3])
	if err != nil {
		return nil, err
	}
	line.Timestamp = ts

	return line, nil
}

func parsePrice(str string) (int, error) {
	parts := strings.SplitN(str, ".", 2)

	i, err := strconv.ParseInt(parts[0], 10, 0)
	if err != nil {
		return 0, err
	}

	price := int(i) * 100
	if len(parts) > 1 {
		cents := parts[1]
		if len(cents) == 1 {
			cents += "0"
		}

		i, err := strconv.ParseInt(cents, 10, 0)
		if err != nil {
			return 0, err
		}
		price += int(i)
	}
	return price, nil
}

func formatPrice(price int) string {
	rem := price % 100
	if rem > 0 {
		if rem % 10 == 0 {
			rem /= 10
		}
		return fmt.Sprintf("%d.%d", price/100, rem)
	}
	return fmt.Sprintf("%d", price/100)
}
