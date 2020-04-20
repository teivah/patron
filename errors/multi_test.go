package errors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testError struct{}

func (t *testError) Error() string {
	return "test"
}

func TestAppend(t *testing.T) {
	var result error
	result = Append(result, errors.New("foo"))
	result = Append(result, errors.New("bar"))
	result = Append(result, errors.New("baz"))
	assert.Equal(t, `foo
bar
baz`, result.Error())
}

func TestAppend_NilErrorsFromMulti(t *testing.T) {
	var result error
	result = Append(result, errors.New("foo"))
	result = Append(result, nil)
	assert.Equal(t, "foo", result.Error())
}

func TestAppend_NilErrorsFromOtherError(t *testing.T) {
	result := Append(&testError{}, nil)
	assert.Equal(t, "test", result.Error())
}

func TestAppend_FromOtherType(t *testing.T) {
	result := errors.New("foo")
	result = Append(result, errors.New("bar"))
	result = Append(result, errors.New("baz"))
	assert.Equal(t, `foo
bar
baz`, result.Error())
}

func Test_Format(t *testing.T) {
	result := &Multi{}
	result.Format = func(errs []error) string {
		s := ""
		for _, err := range errs {
			s += err.Error() + "_"
		}
		return s
	}
	result = Append(result, errors.New("foo"))
	assert.Equal(t, "foo_", result.Error())
}

func Test_As(t *testing.T) {
	result := errors.New("foo")
	result = Append(result, &testError{})
	result = Append(result, errors.New("baz"))
	var e *testError
	assert.True(t, errors.As(result, &e))
}

func Test_Is(t *testing.T) {
	result := errors.New("foo")
	expected := &testError{}
	result = Append(result, expected)
	result = Append(result, errors.New("baz"))
	assert.True(t, errors.Is(result, expected))
}

func TestUnwrap_Empty(t *testing.T) {
	m := &Multi{
		Errors: []error{errors.New("foo")},
	}
	assert.Nil(t, m.Unwrap())
}

func TestErrorOrNil_NotNil(t *testing.T) {
	m := &Multi{
		Errors: []error{errors.New("foo")},
	}
	assert.NotNil(t, m.ErrorOrNil())
}

func TestErrorOrNil_Nil(t *testing.T) {
	assert.Nil(t, getError())
}

func getError() error {
	var m = &Multi{}
	return m.ErrorOrNil()
}
