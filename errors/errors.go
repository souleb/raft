package errors

import (
	"fmt"
)

type Code uint

const (
	OK              Code = 0
	Canceled        Code = 1
	Unknown         Code = 2
	NotFound        Code = 3
	Closed          Code = 4
	Conflict        Code = 5
	InvalidArgument Code = 6
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

func Is(err error, code Code) bool {
	if e, ok := err.(*Error); ok {
		return e.StatusCode == code
	}
	return false
}
