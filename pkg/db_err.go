package pkg

import "fmt"

// DBError is a custom error type for database-related errors.
type DBError struct {
	Err     error  // The underlying error
	Context string // Additional context, e.g., bucket name
}

func (e *DBError) Error() string {
	return fmt.Sprintf("DBError: %v (context: %s)", e.Err, e.Context)
}

// IsNotFound checks if the error is a "bucket not found" error.
func IsNotFound(err error) bool {
	if dbErr, ok := err.(*DBError); ok {
		return dbErr.Err == ErrBucketNotFound
	}
	return false
}

var (
	// ErrBucketNotFound is an error indicating that a bucket was not found.
	ErrBucketNotFound = fmt.Errorf("bucket not found")
)
