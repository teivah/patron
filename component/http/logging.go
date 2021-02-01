package http

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/beatlabs/patron/log"
)

var statusCodes []statusCode

func init() {
	cfg, _ := os.LookupEnv("PATRON_HTTP_STATUS_ERROR_LOGGING")
	c, err := parseStatusCodes(cfg)
	if err != nil {
		log.Fatalf("failed to parse status codes %q: %v", cfg, err)
	}
	statusCodes = c
}

type statusCode struct {
	exactCode  int
	rangeCodes *statusCodeRange
}

func (s statusCode) isRange() bool {
	return s.rangeCodes != nil
}

type statusCodeRange struct {
	start         int
	startInterval intervalType
	end           int
	endInterval   intervalType
}

func (s *statusCodeRange) isIncluded(statusCode int) bool {
	if s.startInterval == included && s.endInterval == included {
		return statusCode >= s.start && statusCode <= s.end
	}
	if s.startInterval == included && s.endInterval == excluded {
		return statusCode >= s.start && statusCode < s.end
	}
	if s.startInterval == excluded && s.endInterval == included {
		return statusCode > s.start && statusCode <= s.end
	}
	return statusCode > s.start && statusCode < s.end
}

type intervalType uint32

const (
	included intervalType = iota
	excluded
)

func parseStatusCodes(cfg string) ([]statusCode, error) {
	splits := strings.Split(cfg, ";")
	statuses := make([]statusCode, len(splits))

	for idx, split := range splits {
		i, err := strconv.Atoi(split)
		isNumber := err == nil
		if isNumber {
			statuses[idx] = statusCode{
				exactCode: i,
			}
		} else {
			codeRange, err := parseRange(split)
			if err != nil {
				return nil, fmt.Errorf("failed to parse status code range %q: %w", split, err)
			}

			statuses[idx] = statusCode{
				rangeCodes: &codeRange,
			}
		}
	}
	return statuses, nil
}

func parseRange(s string) (statusCodeRange, error) {
	// Expected ASCII characters so no need to convert into runes
	if len(s) < 2 {
		return statusCodeRange{}, fmt.Errorf("invalid range %q", s)
	}

	startInterval, err := parseStartInterval(s[0])
	if err != nil {
		return statusCodeRange{}, err
	}

	endInterval, err := parseEndInterval(s[len(s)-1])
	if err != nil {
		return statusCodeRange{}, err
	}

	// Filter interval types
	codes := s[1 : len(s)-1]

	splits := strings.Split(codes, ",")
	if len(splits) != 2 {
		return statusCodeRange{}, fmt.Errorf("invalid status code range: %q", s)
	}

	start, err := strconv.Atoi(splits[0])
	if err != nil {
		return statusCodeRange{}, fmt.Errorf("invalid status code range: %q", s)
	}

	end, err := strconv.Atoi(splits[1])
	if err != nil {
		return statusCodeRange{}, fmt.Errorf("invalid status code range: %q", s)
	}

	return statusCodeRange{
		start:         start,
		startInterval: startInterval,
		end:           end,
		endInterval:   endInterval,
	}, nil
}

func parseStartInterval(c uint8) (intervalType, error) {
	if c == '[' {
		return included, nil
	} else if c == '(' {
		return excluded, nil
	}
	return 0, fmt.Errorf(`invalid interval type %c, expected " or '`, c)
}

func parseEndInterval(c uint8) (intervalType, error) {
	if c == ']' {
		return included, nil
	} else if c == ')' {
		return excluded, nil
	}
	return 0, fmt.Errorf(`invalid interval type %c, expected " or '`, c)
}

func shouldLog(statusCode int, codes []statusCode) bool {
	for _, code := range codes {
		if code.isRange() {
			if code.rangeCodes.isIncluded(statusCode) {
				return true
			}
		} else {
			if statusCode == code.exactCode {
				return true
			}
		}
	}
	return false
}
