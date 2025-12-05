package txn

import "errors"

var (
	// ErrTxNotFound is returned when a transaction ID is not found.
	ErrTxNotFound = errors.New("transaction not found")

	// ErrTxNotActive is returned when trying to commit/abort a non-active transaction.
	ErrTxNotActive = errors.New("transaction is not active")

	// ErrTxAborted is returned when an operation is attempted on an aborted transaction.
	ErrTxAborted = errors.New("transaction has been aborted")

	// ErrSerializationFailure is returned when a transaction cannot be serialized
	// (e.g., write-write conflict in serializable isolation).
	ErrSerializationFailure = errors.New("serialization failure")

	// ErrTupleNotVisible is returned when a tuple is not visible to the current transaction.
	ErrTupleNotVisible = errors.New("tuple not visible")
)
