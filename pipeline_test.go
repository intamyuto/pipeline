package main

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestParsePrice(t *testing.T) {
	tests := []struct {
		in  string
		out int
	}{
		{in: "213.82", out: 21382},
		{in: "213.8", out: 21380},
		{in: "213", out: 21300},
	}

	for _, tt := range tests {
		price, err := parsePrice(tt.in)
		if err != nil {
			t.Fatal(err)
		}
		if price != tt.out {
			t.Fatalf("for %q expected: %d; got: %d", tt.in, tt.out, price)
		}
	}
}

func TestFormatPrice(t *testing.T) {
	tests := []struct {
		in  int
		out string
	}{
		{in: 21382, out: "213.82"},
		{in: 21380, out: "213.8"},
		{in: 21300, out: "213"},
	}

	for _, tt := range tests {
		str := formatPrice(tt.in)
		if str != tt.out {
			t.Fatalf("for %d expected: %s; got: %s", tt.in, tt.out, str)
		}
	}
}

func TestRead(t *testing.T) {
	tests := []struct {
		in  string
		out Line
	}{
		{"SBER,213.8,100,2019-01-30 06:59:45.000249", Line{
			Ticker:    "SBER",
			Timestamp: time.Date(2019, 01, 30, 06, 59, 45, 249000, time.UTC),
			Count:     100,
			Price:     21380,
		}},
		{"AAPL,162.88,2,2019-01-30 07:00:09.838841", Line{
			Ticker:    "AAPL",
			Timestamp: time.Date(2019, 01, 30, 07, 0, 9, 838841000, time.UTC),
			Count:     2,
			Price:     16288,
		}},
	}

	var buf bytes.Buffer
	for _, tt := range tests {
		fmt.Fprintf(&buf, "%s\n", tt.in)
	}

	errch := make(chan error)
	out := make(chan *Line)

	go read(&buf, errch, out)

	for i := 0; i < len(tests); i++ {
		select {
		case err := <-errch:
			t.Fatal(err)
		case line, ok := <-out:
			if !ok {
				t.Fatalf("expected: %d lines; got: %d", len(tests), i+1)
			}

			if tests[i].out != *line {
				t.Fatalf("expected: %+v, got: %+v", tests[i].out, *line)
			}
		}
	}

	select {
	case err := <-errch:
		t.Fatal(err)
	case line, ok := <-out:
		if !ok {
			return
		}
		t.Fatalf("unexpected line: %v", *line)
	}
}

func TestWrite(t *testing.T) {
	text := `SBER,2019-01-30T07:00:00Z,213.8,214.14,213.1,213.17
AAPL,2019-01-30T07:00:00Z,163,163.44,156.25,162.68
`
	candles := []*Candle{
		&Candle{
			Ticker:     "SBER",
			Start:      time.Date(2019, 01, 30, 07, 0, 0, 0, time.UTC),
			PriceStart: 21380,
			PriceMax:   21414,
			PriceMin:   21310,
			PriceEnd:   21317,
		},
		&Candle{
			Ticker:     "AAPL",
			Start:      time.Date(2019, 01, 30, 07, 0, 0, 0, time.UTC),
			PriceStart: 16300,
			PriceMax:   16344,
			PriceMin:   15625,
			PriceEnd:   16268,
		},
	}

	var buf bytes.Buffer
	errch := make(chan error)
	in := make(chan *Candle)
	out := make(chan struct{})

	go write(&buf, errch, in, out)
	go func() {
		for _, candle := range candles {
			in <- candle
		}
		close(in)
	}()

loop:
	for {
		select {
		case err := <-errch:
			t.Fatal(err)
		case <-out:
			break loop
		}
	}

	result := buf.String()
	if result != text {
		t.Fatalf("expected\n%s\ngot\n%s", text, result)
	}
}

func TestProcess(t *testing.T) {
	tests := []struct {
		name string
		in   []*Line
		out  map[string][]*Candle
	}{
		{
			name: "smoke test",
			in: []*Line{
				&Line{
					Ticker:    "AAPL",
					Timestamp: time.Date(2019, 01, 30, 07, 0, 9, 838841000, time.UTC),
					Price:     16288,
				},
			},
			out: map[string][]*Candle{
				"AAPL": []*Candle{
					&Candle{
						Ticker:     "AAPL",
						Start:      time.Date(2019, 01, 30, 07, 0, 0, 0, time.UTC),
						PriceStart: 16288,
						PriceMax:   16288,
						PriceMin:   16288,
						PriceEnd:   16288,
					},
				},
			},
		},
		{
			name: "split",
			in: []*Line{
				&Line{
					Ticker:    "AAPL",
					Timestamp: time.Date(2019, 01, 30, 07, 0, 9, 838841000, time.UTC),
					Price:     16288,
				},
				&Line{
					Ticker:    "AAPL",
					Timestamp: time.Date(2019, 01, 30, 07, 07, 33, 21484000, time.UTC),
					Price:     16148,
				},
			},
			out: map[string][]*Candle{
				"AAPL": []*Candle{
					&Candle{
						Ticker:     "AAPL",
						Start:      time.Date(2019, 01, 30, 07, 0, 0, 0, time.UTC),
						PriceStart: 16288,
						PriceMax:   16288,
						PriceMin:   16288,
						PriceEnd:   16288,
					},
					&Candle{
						Ticker:     "AAPL",
						Start:      time.Date(2019, 01, 30, 07, 5, 0, 0, time.UTC),
						PriceStart: 16148,
						PriceMax:   16148,
						PriceMin:   16148,
						PriceEnd:   16148,
					},
				},
			},
		},
		{
			name: "group",
			in: []*Line{
				&Line{
					Ticker:    "SBER",
					Timestamp: time.Date(2019, 01, 30, 07, 0, 1, 53224000, time.UTC),
					Price:     21380,
				},
				&Line{
					Ticker:    "AAPL",
					Timestamp: time.Date(2019, 01, 30, 07, 0, 9, 838841000, time.UTC),
					Price:     16288,
				},
			},
			out: map[string][]*Candle{
				"AAPL": []*Candle{
					&Candle{
						Ticker:     "AAPL",
						Start:      time.Date(2019, 01, 30, 07, 0, 0, 0, time.UTC),
						PriceStart: 16288,
						PriceMax:   16288,
						PriceMin:   16288,
						PriceEnd:   16288,
					},
				},
				"SBER": []*Candle{
					&Candle{
						Ticker:     "SBER",
						Start:      time.Date(2019, 01, 30, 07, 0, 0, 0, time.UTC),
						PriceStart: 21380,
						PriceMax:   21380,
						PriceMin:   21380,
						PriceEnd:   21380,
					},
				},
			},
		},
		{
			name: "aggregate",
			in: []*Line{
				&Line{
					Ticker:    "AAPL",
					Timestamp: time.Date(2019, 01, 30, 07, 0, 2, 949487000, time.UTC),
					Price:     16320,
				},
				&Line{
					Ticker:    "AAPL",
					Timestamp: time.Date(2019, 01, 30, 07, 0, 9, 838841000, time.UTC),
					Price:     16288,
				},
				&Line{
					Ticker:    "AAPL",
					Timestamp: time.Date(2019, 01, 30, 07, 0, 10, 170781000, time.UTC),
					Price:     16290,
				},
			},
			out: map[string][]*Candle{
				"AAPL": []*Candle{
					&Candle{
						Ticker:     "AAPL",
						Start:      time.Date(2019, 01, 30, 07, 0, 0, 0, time.UTC),
						PriceStart: 16320,
						PriceMax:   16320,
						PriceMin:   16288,
						PriceEnd:   16290,
					},
				},
			},
		},
		{
			name: "filter",
			in: []*Line{
				&Line{
					Ticker:    "SBER",
					Timestamp: time.Date(2019, 01, 30, 06, 59, 45, 249000, time.UTC),
					Price:     21390,
				},
				&Line{
					Ticker:    "SBER",
					Timestamp: time.Date(2019, 01, 30, 07, 0, 1, 53224000, time.UTC),
					Price:     21380,
				},
			},
			out: map[string][]*Candle{
				"SBER": []*Candle{
					&Candle{
						Ticker:     "SBER",
						Start:      time.Date(2019, 01, 30, 07, 0, 0, 0, time.UTC),
						PriceStart: 21380,
						PriceMax:   21380,
						PriceMin:   21380,
						PriceEnd:   21380,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		in := make(chan *Line)
		out := make(chan *Candle)

		go process(5*time.Minute, in, out)
		go func() {
			for _, line := range tt.in {
				in <- line
			}
			close(in)
		}()

		result := make(map[string][]*Candle)
		for candle := range out {
			result[candle.Ticker] = append(result[candle.Ticker], candle)
		}

		for k, v := range tt.out {
			cs, ok := result[k]
			if !ok {
				t.Fatalf("%s: missing candle with ticker '%s'", tt.name, k)
			}

			if len(v) != len(cs) {
				t.Fatalf("%s: expected %d candles for %s; got: %d", tt.name, len(v), k, len(cs))
			}

			for i, c := range v {
				if *c != *cs[i] {
					t.Fatalf("%s:\nexpected\n%+v\ngot\n%+v", tt.name, *c, *cs[i])
				}
			}

			delete(result, k)
		}

		for k, _ := range result {
			t.Fatalf("%s: unexpected candle with ticker '%s'", tt.name, k)
		}
	}

}
