package bunnymq

import "fmt"

// DBErrorCode represents a specific error code for database-related errors.
type DBErrorCode int

const (
	CodeUnknown DBErrorCode = iota
	CodeBucketNotFound
	CodeKeyNotFound
	CodeNoMoreMessages
	CodeInvalidProgress
	CodeFailedToDeserialize
	CodeFailedToCreate
	CodeRenamingDatabaseFile
	CodeReopeningDatabase
	CodeOpeningBackupDatabase
	CodeFailedToDelete
	CodeClosingDatabase
	CodeDeletingBackupFile
	CodeFailToStore
)

// DBError is a custom error type for database-related errors.
type DBError struct {
	Code    DBErrorCode // The specific error code
	Err     error       // The underlying error
	Context string      // Additional context, e.g., bucket name or operation type
}

func (e *DBError) Error() string {
	return fmt.Sprintf("DBError: %v (context: %s) [Code: %d]", e.Err, e.Context, e.Code)
}

// Unwrap returns the underlying error, allowing errors.Is and errors.As to work with DBError.
func (e *DBError) Unwrap() error {
	return e.Err
}

func NewDBError(code DBErrorCode, err error, context string) *DBError {
	return &DBError{
		Code:    code,
		Err:     err,
		Context: context,
	}
}

// Predefined database-related errors with codes
var (
	ErrBucketNotFound        = NewDBError(CodeBucketNotFound, fmt.Errorf("bucket not found"), "")
	ErrKeyNotFound           = NewDBError(CodeKeyNotFound, fmt.Errorf("key not found"), "")
	ErrNoMoreMessages        = NewDBError(CodeNoMoreMessages, fmt.Errorf("no more messages in queue"), "")
	ErrInvalidProgress       = NewDBError(CodeInvalidProgress, fmt.Errorf("invalid progress"), "")
	ErrFailedToDeserialize   = NewDBError(CodeFailedToDeserialize, fmt.Errorf("failed to deserialize data"), "")
	ErrFailedToCreate        = NewDBError(CodeFailedToCreate, fmt.Errorf("failed to create resource"), "")
	ErrRenamingDatabaseFile  = NewDBError(CodeRenamingDatabaseFile, fmt.Errorf("failed to rename database file"), "")
	ErrReopeningDatabase     = NewDBError(CodeReopeningDatabase, fmt.Errorf("failed to reopen database"), "")
	ErrOpeningBackupDatabase = NewDBError(CodeOpeningBackupDatabase, fmt.Errorf("failed to open backup database"), "")
	ErrFailedToDelete        = NewDBError(CodeFailedToDelete, fmt.Errorf("failed to delete key"), "")
	ErrDeDeletingBackupFile  = NewDBError(CodeDeletingBackupFile, fmt.Errorf("deleting existing backup file"), "")
	ErrClosingDatabase       = NewDBError(CodeDeletingBackupFile, fmt.Errorf("dclosing databas"), "")
	ErrFailToStore           = NewDBError(CodeFailToStore, fmt.Errorf("failed to store data"), "")
)
