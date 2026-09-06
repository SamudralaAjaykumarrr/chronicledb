package raft

import "errors"

// ErrInvalidConfig is returned by NewCore when Config fails validation.
var ErrInvalidConfig = errors.New("raft: invalid config")
