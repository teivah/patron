package errors

import (
	"errors"
	"strings"
)

// Multi is an error implementation that allows to handle multiple errors.
type Multi struct {
	Errors []error
	Format func([]error) string
}

// Error stringify the error.
// If a format was specified, we use it instead of the default implementation.
func (m *Multi) Error() string {
	if m.Format != nil {
		return m.Format(m.Errors)
	}

	sb := strings.Builder{}
	for i := 0; i < len(m.Errors); i++ {
		sb.WriteString(m.Errors[i].Error())
		if i != len(m.Errors)-1 {
			sb.WriteRune('\n')
		}
	}
	return sb.String()
}

// ErrorOrNil returns an error only if the internal slice is not empty.
// Otherwise, it returns nil.
// This utility method is used to avoid the insidious bug while returning a nil structure converted into an interface.
func (m *Multi) ErrorOrNil() error {
	if m == nil || len(m.Errors) == 0 {
		return nil
	}
	return m
}

// Unwrap unwraps a Multi structure by returning a subset of the internal errors.
func (m *Multi) Unwrap() error {
	if len(m.Errors) <= 1 {
		return nil
	}
	return &Multi{
		Errors: m.Errors[1:],
		Format: m.Format,
	}
}

// As implements the standard As function from the errors library.
func (m *Multi) As(target interface{}) bool {
	return errors.As(m.Errors[0], target)
}

// Is implements the standard Is function from the errors library.
func (m *Multi) Is(target error) bool {
	return errors.Is(m.Errors[0], target)
}

// Append returns a Multi error that appends a list of errors to a current error.
// The current error can be nil.
func Append(current error, err error) *Multi {
	if current == nil {
		current = &Multi{}
	}

	if err == nil {
		switch t := current.(type) {
		default:
			return &Multi{
				Errors: []error{current},
			}
		case *Multi:
			return t
		}
	}

	switch result := current.(type) {
	default:
		m := &Multi{}
		m.Errors = append([]error{result}, err)
		return m
	case *Multi:
		result.Errors = append(result.Errors, err)
		return result
	}
}
