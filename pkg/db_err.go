package pkg

import "fmt"

// DBError is a custom error type for database-related errors.
type DBError struct {
	Err     error  // The underlying error
	Context string // Additional context, e.g., bucket name or operation type
}

func (e *DBError) Error() string {
	return fmt.Sprintf("DBError: %v (context: %s)", e.Err, e.Context)
}

// Unwrap returns the underlying error, allowing errors.Is and errors.As to work with DBError.
func (e *DBError) Unwrap() error {
	return e.Err
}

func NewDBError(err error, context string) *DBError {
	return &DBError{
		Err:     err,
		Context: context,
	}
}

var (
	// ErrBucketNotFound is an error indicating that a bucket was not found.
	ErrBucketNotFound = fmt.Errorf("bucket not found")

	// ErrKeyNotFound is an error indicating that a key was not found in the bucket.
	ErrKeyNotFound = fmt.Errorf("key not found")
)
