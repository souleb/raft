package errors

import (
	"fmt"
)

type Code uint

const (
	OK       Code = 0
	Canceled Code = 1
	Unknown  Code = 2
)

type Error struct {
	StatusCode Code
	Err        error
}

func (e *Error) Error() string {
	return fmt.Sprintf("status %d: err %v", e.StatusCode, e.Err)
}

func (e *Error) Unwrap() error {
	return e.Err
}
