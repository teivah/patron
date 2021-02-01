package http

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

const complexConfig = "200;(210,212);(220,222];[230,232);[240,242]"

func TestStatusCode(t *testing.T) {
	type args struct {
		cfg        string
		statusCode int
	}
	tests := map[string]struct {
		args           args
		expectedResult bool
		expectedErr    error
	}{
		"empty":                                    {args: args{cfg: "", statusCode: 400}, expectedResult: false},
		"single element - true":                    {args: args{cfg: "400", statusCode: 400}, expectedResult: true},
		"single element - false":                   {args: args{cfg: "400", statusCode: 401}, expectedResult: false},
		"complex config - single element - true":   {args: args{cfg: complexConfig, statusCode: 200}, expectedResult: true},
		"complex config - single element - false":  {args: args{cfg: complexConfig, statusCode: 300}, expectedResult: false},
		"complex config - excl/excl range - true":  {args: args{cfg: complexConfig, statusCode: 211}, expectedResult: true},
		"complex config - excl/excl range - false": {args: args{cfg: complexConfig, statusCode: 212}, expectedResult: false},
		"complex config - excl/incl range - true":  {args: args{cfg: complexConfig, statusCode: 222}, expectedResult: true},
		"complex config - excl/incl range - false": {args: args{cfg: complexConfig, statusCode: 220}, expectedResult: false},
		"complex config - incl/excl range - true":  {args: args{cfg: complexConfig, statusCode: 230}, expectedResult: true},
		"complex config - incl/excl range - false": {args: args{cfg: complexConfig, statusCode: 232}, expectedResult: false},
		"complex config - incl/incl range - true":  {args: args{cfg: complexConfig, statusCode: 240}, expectedResult: true},
		"complex config - incl/incl range - false": {args: args{cfg: complexConfig, statusCode: 243}, expectedResult: false},
		"config error":                             {args: args{cfg: "[200,]"}, expectedErr: errors.New(`failed to parse status code range "[200,]": invalid status code range: "[200,]"`)},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			codes, err := parseStatusCodes(tt.args.cfg)
			if tt.expectedErr != nil {
				assert.EqualError(t, err, tt.expectedErr.Error())
			} else {
				got := shouldLog(tt.args.statusCode, codes)
				assert.Equal(t, tt.expectedResult, got)
			}
		})
	}
}

func BenchmarkName(b *testing.B) {
	for i := 0; i < b.N; i++ {

	}
}
