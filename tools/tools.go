package tools

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
    "runtime"
)

// StatusType defines the possible status values for steps and the overall pipeline.
type StatusType string

const (
	StatusPending   StatusType = "Pending"
	StatusRunning   StatusType = "Running"
	StatusCompleted StatusType = "Completed"
	StatusFailed    StatusType = "Failed"
	StatusRetrying  StatusType = "Retrying" // Optional: if you plan to implement retries
	StatusSkipped   StatusType = "Skipped"  // Optional: if steps can be skipped
)

// StepStatus holds the status of an individual ETL step.
type StepStatus struct {
	StepName       string     `json:"stepName"`
	Status         StatusType `json:"status"` // e.g., "Pending", "Running", "Completed", "Failed"
	StartTime      time.Time  `json:"startTime"`
	EndTime        time.Time  `json:"endTime,omitempty"`
	DurationMillis int64      `json:"durationMillis,omitempty"`
	Message        string     `json:"message,omitempty"` // For errors or other info
}

// PipelineRun holds the overall status of an ETL pipeline execution.
type PipelineRun struct {
	RunID          string         `json:"runId"`
	StartTime      time.Time      `json:"startTime"`
	OverallStatus  StatusType     `json:"overallStatus"`
	Steps          []StepStatus   `json:"steps"`
	stepMap        map[string]int `json:"-"` // Internal map for quick step lookup by name, not part of saved JSON
	StatusFilePath string         `json:"-"` // Runtime configuration, not part of saved JSON
}

// NewPipelineRun initializes a new pipeline run.
func NewPipelineRun(statusFilePath string) *PipelineRun {
	return &PipelineRun{
		RunID:          fmt.Sprintf("run_%s", time.Now().Format("20060102_150405.000")), // Added milliseconds
		StartTime:      time.Now(),
		OverallStatus:  StatusPending, // Will change to Running when the first step starts
		Steps:          make([]StepStatus, 0),
		stepMap:        make(map[string]int),
		StatusFilePath: statusFilePath,
	}
}

// ExecuteStep runs a single ETL step, handling status updates, logging, and error exiting.
func (pr *PipelineRun) ExecuteStep(stepName string, stepFunc func() error) {
	pr.StartStep(stepName) // This will set OverallStatus to Running if it's Pending

	err := stepFunc()

	pr.EndStep(stepName, err)
	pr.LogStatus() // Log status after each step execution (attempt)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error in step '%s': %v\n", stepName, err)
		if pr.StatusFilePath != "" {
			if saveErr := pr.SaveStatus(pr.StatusFilePath); saveErr != nil {
				fmt.Fprintf(os.Stderr, "Additionally, failed to save status to %s after error in step '%s': %v\n", pr.StatusFilePath, stepName, saveErr)
			}
		} else {
			fmt.Fprintf(os.Stderr, "Warning: StatusFilePath not set in PipelineRun, cannot save status after error in step '%s'.\n", stepName)
		}
		os.Exit(1) // Exit the pipeline on the first error
	}
}

// StartStep adds or updates a step, sets its status to "Running", and records StartTime.
func (pr *PipelineRun) StartStep(stepName string) {
	now := time.Now()
	if idx, exists := pr.stepMap[stepName]; exists {
		// If step exists, update it (e.g., if it's being retried)
		pr.Steps[idx].Status = StatusRunning
		pr.Steps[idx].StartTime = now
		pr.Steps[idx].EndTime = time.Time{} // Clear previous end time
		pr.Steps[idx].DurationMillis = 0
		pr.Steps[idx].Message = ""
	} else {
		newStep := StepStatus{
			StepName:  stepName,
			Status:    StatusRunning,
			StartTime: now,
		}
		pr.Steps = append(pr.Steps, newStep)
		pr.stepMap[stepName] = len(pr.Steps) - 1
	}
	pr.OverallStatus = StatusRunning // Ensure pipeline status reflects activity
}

// EndStep updates a step's status, records EndTime, calculates DurationMillis, and stores an error message if any.
func (pr *PipelineRun) EndStep(stepName string, err error) {
	now := time.Now()
	idx, exists := pr.stepMap[stepName]
	if !exists {
		// This shouldn't happen if StartStep was called correctly
		// but handle it defensively.
		newStep := StepStatus{
			StepName:  stepName,
			StartTime: now, // Or some default, as actual start is unknown
			EndTime:   now,
		}
		if err != nil {
			newStep.Status = StatusFailed
			newStep.Message = err.Error()
			pr.OverallStatus = StatusFailed
		} else {
			newStep.Status = StatusCompleted
		}
		pr.Steps = append(pr.Steps, newStep)
		pr.stepMap[stepName] = len(pr.Steps) - 1
		return
	}

	pr.Steps[idx].EndTime = now
	pr.Steps[idx].DurationMillis = pr.Steps[idx].EndTime.Sub(pr.Steps[idx].StartTime).Milliseconds()

	if err != nil {
		pr.Steps[idx].Status = StatusFailed
		pr.Steps[idx].Message = err.Error()
		pr.OverallStatus = StatusFailed // If any step fails, the pipeline fails
	} else {
		pr.Steps[idx].Status = StatusCompleted
		// Check if all other steps are completed to set overall status
		if pr.OverallStatus != StatusFailed {
			allCompleted := true
			for _, step := range pr.Steps {
				if step.Status != StatusCompleted {
					allCompleted = false
					break
				}
			}
			if allCompleted {
				pr.OverallStatus = StatusCompleted
			} else {
				pr.OverallStatus = StatusRunning // Or "PartiallyCompleted" if you want more granularity
			}
		}
	}
}

// LogStatus prints the current status of the pipeline and its steps to the console.
func (pr *PipelineRun) LogStatus() {
	fmt.Printf("\n--- Pipeline Run Status ---\n")
	fmt.Printf("Run ID: %s\n", pr.RunID)
	fmt.Printf("Overall Status: %s\n", pr.OverallStatus)
	fmt.Printf("Start Time: %s\n", pr.StartTime.Format(time.RFC3339))
	if pr.OverallStatus == StatusCompleted || pr.OverallStatus == StatusFailed {
		// Find the last EndTime among all steps for the pipeline's end time
		var pipelineEndTime time.Time
		if len(pr.Steps) > 0 {
			pipelineEndTime = pr.Steps[0].EndTime
			for _, step := range pr.Steps {
				if step.EndTime.After(pipelineEndTime) {
					pipelineEndTime = step.EndTime
				}
			}
		}
		if !pipelineEndTime.IsZero() {
			fmt.Printf("End Time: %s\n", pipelineEndTime.Format(time.RFC3339))
			fmt.Printf("Total Duration: %s\n", pipelineEndTime.Sub(pr.StartTime).Round(time.Millisecond).String())
		}
	}
	fmt.Println("Steps:")
	if len(pr.Steps) == 0 {
		fmt.Println("  No steps initiated yet.")
	}
	for _, step := range pr.Steps {
		fmt.Printf("  - Step: %s\n", step.StepName)
		fmt.Printf("    Status: %s\n", step.Status)
		fmt.Printf("    Start Time: %s\n", step.StartTime.Format(time.RFC3339))
		if !step.EndTime.IsZero() {
			fmt.Printf("    End Time: %s\n", step.EndTime.Format(time.RFC3339))
			fmt.Printf("    Duration: %d ms\n", step.DurationMillis)
		}
		if step.Message != "" {
			fmt.Printf("    Message: %s\n", step.Message)
		}
	}
	fmt.Println("-------------------------")
}

// SaveStatus saves the PipelineRun struct as JSON to a file.
func (pr *PipelineRun) SaveStatus(filePath string) error {
	data, err := json.MarshalIndent(pr, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal pipeline status: %w", err)
	}
	err = os.WriteFile(filePath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write pipeline status to file %s: %w", filePath, err)
	}
	fmt.Printf("Pipeline status saved to %s\n", filePath)
	return nil
}

const (
	TempBaseDir   = "temp"
	OutputBaseDir = "output"
	// Default name for the main data file within a versioned directory, if needed.
	// However, the prompt "1.json, 2.json" suggests files are directly named by version.
	// VersionedDataFileName = "data.json"
)

// EnsureDir ensures that the specified directory path exists.
// If it does not exist, it creates it, including any necessary parents.
func EnsureDir(dirPath string) error {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dirPath, err)
	}
	return nil
}

// EnsureDirForFile ensures that the parent directory for the given file path exists.
func EnsureDirForFile(filePath string) error {
	dir := filepath.Dir(filePath)
	return EnsureDir(dir)
}

// GetTempFilePath returns a path for a temporary file within the temp directory,
// structured under a subdirectory named after the step.
// It ensures the subdirectory for the step exists.
// Example: temp/MyStepName/some_temp_file.txt
func GetTempFilePath(stepName string, fileName string) (string, error) {
	if stepName == "" {
		return "", fmt.Errorf("stepName cannot be empty for temp file path")
	}
	if fileName == "" {
		return "", fmt.Errorf("fileName cannot be empty for temp file path")
	}
	dir := filepath.Join(TempBaseDir, stepName)
	if err := EnsureDir(dir); err != nil {
		return "", err
	}
	return filepath.Join(dir, fileName), nil
}

// CleanDirContents removes all files and subdirectories directly within dirPath,
// but not dirPath itself. It skips any paths provided in the exceptions list.
// Exceptions should be full paths.
func CleanDirContents(dirPath string, exceptions []string) error {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Directory doesn't exist, nothing to clean
		}
		return fmt.Errorf("failed to read directory %s: %w", dirPath, err)
	}

	exceptionSet := make(map[string]bool)
	for _, exc := range exceptions {
		exceptionSet[exc] = true
	}

	for _, entry := range entries {
		fullPath := filepath.Join(dirPath, entry.Name())
		if exceptionSet[fullPath] {
			continue // Skip exceptions
		}
		if err := os.RemoveAll(fullPath); err != nil {
			return fmt.Errorf("failed to remove %s: %w", fullPath, err)
		}
	}
	return nil
}

// getNextVersionNumber scans a directory for files named "N.jsonl" (e.g., "1.jsonl", "2.jsonl")
// and returns the next available version number (max existing version + 1).
// If no versioned files exist, it returns 1.
func getNextVersionNumber(dirPath string) (int, error) {
	if err := EnsureDir(dirPath); err != nil { // Ensure directory exists before reading
		return 0, err
	}
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return 0, fmt.Errorf("failed to read directory %s for versioning: %w", dirPath, err)
	}

	maxVersion := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			name := entry.Name()
			if strings.HasSuffix(name, ".jsonl") {
				baseName := strings.TrimSuffix(name, ".jsonl")
				version, err := strconv.Atoi(baseName)
				if err == nil && version > maxVersion { // Successfully parsed an integer
					maxVersion = version
				}
			}
		}
	}
	return maxVersion + 1, nil
}

// GetNextVersionedFilePath returns the path for the next versioned output file.
// Files are named "N.jsonl" (e.g., "1.jsonl", "2.jsonl").
// Path structure: output/<flowType>/<stepName>/<versionNumber>.jsonl
// It also returns the determined version number.
func GetNextVersionedFilePath(stepName string) (string, int, error) {
	if stepName == "" {
		return "", 0, fmt.Errorf("flowType and stepName cannot be empty for versioned file path")
	}
	dir := filepath.Join(OutputBaseDir, stepName)
	if err := EnsureDir(dir); err != nil {
		return "", 0, err
	}

	version, err := getNextVersionNumber(dir) // Removed extension parameter
	if err != nil {
		return "", 0, err
	}

	filePath := filepath.Join(dir, fmt.Sprintf("%d.jsonl", version))
	return filePath, version, nil
}

// GetLatestVersionedFilePath finds the path to the most recent versioned file ("N.jsonl")
// in the directory output/<flowType>/<stepName>/.
// It returns the full file path and the version number.
// Returns an error if no versioned files are found.
func GetLatestVersionedFilePath(stepName string) (string, int, error) {
	if stepName == "" {
		return "", 0, fmt.Errorf("flowType and stepName cannot be empty for latest versioned file path")
	}
	dir := filepath.Join(OutputBaseDir, stepName)

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", 0, fmt.Errorf("versioned directory %s does not exist", dir)
		}
		return "", 0, fmt.Errorf("failed to read directory %s for latest version: %w", dir, err)
	}

	latestVersion := 0
	found := false
	for _, entry := range entries {
		if !entry.IsDir() {
			name := entry.Name()
			if strings.HasSuffix(name, ".jsonl") { // Hardcoded .jsonl
				baseName := strings.TrimSuffix(name, ".jsonl") // Hardcoded .jsonl
				version, err := strconv.Atoi(baseName)
				if err == nil {
					if version > latestVersion {
						latestVersion = version
						found = true
					}
				}
			}
		}
	}

	if !found {
		return "", 0, fmt.Errorf("no versioned .jsonl files found in %s", dir)
	}

	filePath := filepath.Join(dir, fmt.Sprintf("%d.jsonl", latestVersion)) // Hardcoded .jsonl
	return filePath, latestVersion, nil
}

// GetSpecificVersionedFilePath returns the path for a specific versioned file.
// Path structure: output/<flowType>/<stepName>/<versionNumber>.jsonl
func GetSpecificVersionedFilePath(stepName string, version int) (string, error) {
	if stepName == "" {
		return "", fmt.Errorf("flowType and stepName cannot be empty")
	}
	if version <= 0 {
		return "", fmt.Errorf("version number must be positive")
	}
	dir := filepath.Join(OutputBaseDir, stepName)
	filePath := filepath.Join(dir, fmt.Sprintf("%d.jsonl", version)) // Hardcoded .jsonl
	return filePath, nil
}

// GetNextVersionedJSONLWriter gets the path for the next versioned .jsonl output file
// and returns a json.Encoder to write to it, along with a closer function and the version number.
// It handles errors from path generation and writer creation.
// GetNextVersionedJSONLWriter gets the path for the next versioned .jsonl output file
// and returns a json.Encoder to write to it, along with a closer function, the version number, and the file path.
// If stepName is empty, it uses the caller function's name as the step name.
func GetNextVersionedJSONLWriter(stepName ...string) (encoder *json.Encoder, closer func() error, version int, filePath string, err error) {
    var actualStepName string
    if len(stepName) > 0 && stepName[0] != "" {
        actualStepName = stepName[0]
    } else {
        actualStepName = getCallerFunctionName(2)
    }

    filePath, version, err = GetNextVersionedFilePath(actualStepName)
    if err != nil {
        err = fmt.Errorf("getting next versioned .jsonl file path for %s/%s: %w", actualStepName, err)
        return
    }

    encoder, closer, err = NewJSONLWriter(filePath)
    if err != nil {
        err = fmt.Errorf("creating JSONL writer for %s (version %d): %w", filePath, version, err)
        return
    }

    return encoder, closer, version, filePath, nil
}

// SaveJSON marshals the given data to JSON and writes it to the specified file path.
// It ensures the directory for the file exists.
// NOTE: This function saves standard JSON. If you need to save JSONL using this generic
// function, the 'data' interface would need to be a slice of items, and you'd iterate
// and encode each one, similar to NewJSONLWriter.
// For single JSON objects (like a summary/manifest), .json is fine.
// If your versioned data files from Extract/Load are always JSONL,
// then the GetNextVersionedFilePath etc. should use .jsonl.
// The SaveJSON/ReadJSON are generic for standard JSON objects/arrays.
func SaveJSON(filePath string, data interface{}) error {
	if err := EnsureDirForFile(filePath); err != nil {
		return err
	}
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal data to JSON for %s: %w", filePath, err)
	}
	err = os.WriteFile(filePath, jsonData, 0644)
	if err != nil {
		return fmt.Errorf("failed to write JSON data to file %s: %w", filePath, err)
	}
	return nil
}

// ReadJSON reads a JSON file from the specified path and unmarshals it into the target interface.
// NOTE: This function reads standard JSON. For JSONL, use StreamJSONLRecords or ReadAllJSONLFile.
func ReadJSON(filePath string, target interface{}) error {
	jsonData, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read JSON file %s: %w", filePath, err)
	}
	err = json.Unmarshal(jsonData, target)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON data from %s: %w", filePath, err)
	}
	return nil
}

// ReadLatestVersionedJSONL reads the latest versioned JSONL file
// for a given flowType and stepName.
// It unmarshals all records into the target slice.
func ReadLatestVersionedJSONL(flowType string, stepName string, targetSlicePtr interface{}) (int, error) {
	filePath, version, err := GetLatestVersionedFilePath(stepName)
	if err != nil {
		return 0, fmt.Errorf("getting latest versioned .jsonl file path for %s/%s: %w", flowType, stepName, err)
	}

	err = ReadAllJSONLFile(filePath, targetSlicePtr)
	if err != nil {
		return 0, fmt.Errorf("reading latest versioned .jsonl file %s: %w", filePath, err)
	}
	return version, nil
}

// ReadSpecificVersionedJSONL reads a specific versioned JSONL file
// for a given flowType, stepName, and version.
// It unmarshals all records into the target slice.
func ReadSpecificVersionedJSONL(flowType string, stepName string, version int, targetSlicePtr interface{}) error {
	filePath, err := GetSpecificVersionedFilePath(stepName, version)
	if err != nil {
		return fmt.Errorf("getting specific versioned .jsonl file path for %s/%s v%d: %w", flowType, stepName, version, err)
	}

	err = ReadAllJSONLFile(filePath, targetSlicePtr)
	if err != nil {
		return fmt.Errorf("reading specific versioned .jsonl file %s: %w", filePath, err)
	}
	return nil
}

// NewJSONLWriter opens/creates a file for writing line-delimited JSON.
// It returns a json.Encoder to write individual records and a closer function.
// The caller is responsible for calling the closer function, typically in a defer statement.
func NewJSONLWriter(filePath string) (*json.Encoder, func() error, error) {
	if err := EnsureDirForFile(filePath); err != nil {
		return nil, nil, fmt.Errorf("ensuring directory for %s: %w", filePath, err)
	}

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("opening file %s for JSONL writing: %w", filePath, err)
	}

	encoder := json.NewEncoder(file)

	closer := func() error {
		return file.Close()
	}

	return encoder, closer, nil
}

// StreamJSONLRecords reads a JSONL file line by line, unmarshaling each line
// into a new instance of the type of recordTemplate, and calls onRecord for each.
// recordTemplate is used to determine the type of each record.
func StreamJSONLRecords(filePath string, recordTemplate interface{}, onRecord func(record interface{}) error) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("opening file %s for JSONL reading: %w", filePath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	recordType := reflect.TypeOf(recordTemplate)
	if recordType.Kind() == reflect.Ptr { // Ensure we have the underlying type if a pointer was passed
		recordType = recordType.Elem()
	}

	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		lineBytes := scanner.Bytes()
		if len(strings.TrimSpace(string(lineBytes))) == 0 { // Skip empty lines
			continue
		}

		// Create a new instance of the record type for unmarshaling
		// newRecordPtr will be a pointer to a new value of recordType (e.g., *MyStruct)
		newRecordPtr := reflect.New(recordType).Interface()

		if err := json.Unmarshal(lineBytes, newRecordPtr); err != nil {
			return fmt.Errorf("unmarshaling line %d from %s: %w. Line content: %s", lineNumber, filePath, err, string(lineBytes))
		}

		// Pass the unmarshaled record (value, not pointer, unless recordTemplate was a pointer type itself)
		// If recordTemplate was MyStruct{}, newRecordPtr is *MyStruct, so reflect.ValueOf(newRecordPtr).Elem().Interface() gives MyStruct.
		// If recordTemplate was &MyStruct{}, recordType is MyStruct, newRecordPtr is *MyStruct, so reflect.ValueOf(newRecordPtr).Interface() is *MyStruct.
		// For simplicity and consistency, let's assume onRecord expects the actual value or a pointer if recordTemplate was a pointer.
		// The current implementation passes a pointer to the new instance.
		// If onRecord expects a value type, it should be reflect.ValueOf(newRecordPtr).Elem().Interface()
		// Let's make onRecord always receive a pointer to the unmarshaled object for consistency.
		if err := onRecord(newRecordPtr); err != nil {
			return fmt.Errorf("processing record from line %d of %s: %w", lineNumber, filePath, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning file %s: %w", filePath, err)
	}
	return nil
}

// ReadAllJSONLFile reads all records from a JSONL file into a slice.
// The targetSlicePtr must be a pointer to a slice of the desired struct type (e.g., *[]MyStruct).
func ReadAllJSONLFile(filePath string, targetSlicePtr interface{}) error {
	slicePtrValue := reflect.ValueOf(targetSlicePtr)
	if slicePtrValue.Kind() != reflect.Ptr {
		return fmt.Errorf("targetSlicePtr must be a pointer to a slice, got %T", targetSlicePtr)
	}

	sliceValue := slicePtrValue.Elem()
	if sliceValue.Kind() != reflect.Slice {
		return fmt.Errorf("targetSlicePtr must be a pointer to a slice, got pointer to %s", sliceValue.Kind())
	}

	// Get the type of the elements in the slice (e.g., MyStruct from *[]MyStruct)
	elemType := sliceValue.Type().Elem()

	// Reset the slice to be empty before appending, in case it's reused
	sliceValue.Set(reflect.MakeSlice(sliceValue.Type(), 0, 0))

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("opening file %s for JSONL reading: %w", filePath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		lineBytes := scanner.Bytes()
		if len(strings.TrimSpace(string(lineBytes))) == 0 { // Skip empty lines
			continue
		}

		// Create a new instance of the element type (e.g., MyStruct)
		newElemPtr := reflect.New(elemType) // This is a pointer to the element type

		if err := json.Unmarshal(lineBytes, newElemPtr.Interface()); err != nil {
			return fmt.Errorf("unmarshaling line %d from %s: %w. Line content: %s", lineNumber, filePath, err, string(lineBytes))
		}

		// Append the new element (dereferenced pointer) to the slice
		sliceValue.Set(reflect.Append(sliceValue, newElemPtr.Elem()))
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning file %s: %w", filePath, err)
	}
	return nil
}

// Knoll performs initial setup tasks for the ETL pipeline:
// - Prints a starting message.
// - Ensures the temporary directory exists.
// - Cleans the temporary directory (for a fresh run).
func Knoll() {
	fmt.Println("Starting ETL Pipeline...")
	if err := EnsureDir(TempBaseDir); err != nil {
		// If we can't even ensure the temp dir, it's a more serious warning.
		fmt.Fprintf(os.Stderr, "Critical Warning: could not ensure temp directory %s: %v\n", TempBaseDir, err)
		// Depending on requirements, you might choose to os.Exit(1) here
	} else {
		// Only clean if EnsureDir was successful or didn't error out (e.g. already exists)
		if err := CleanDirContents(TempBaseDir, nil); err != nil { // Clean all for a fresh run
			fmt.Fprintf(os.Stderr, "Warning: could not clean temp directory %s: %v\n", TempBaseDir, err)
		}
	}
}

// Stow performs final tasks for a successfully completed ETL pipeline:
// - Prints a completion message.
// - Saves the final status of the pipeline run.
func (pr *PipelineRun) Stow() {
	fmt.Println("ETL Pipeline completed successfully.")
	if pr.StatusFilePath == "" {
		fmt.Fprintf(os.Stderr, "Warning: StatusFilePath not set in PipelineRun, cannot save final status.\n")
		return
	}
	if err := pr.SaveStatus(pr.StatusFilePath); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to save final successful status to %s: %v\n", pr.StatusFilePath, err)
	}
}

func getCallerFunctionName(skip int) string {
    // Skip GetCallerFunctionName and the function that called it (runtime.Caller)
    // to get the actual caller of the function that *uses* getCallerFunctionName.
    // pc, _, _, ok := runtime.Caller(2) // Skip 2 frames
    // If getCallerFunctionName is called directly by the function we want to identify,
    // then skip = 1 (for runtime.Caller itself) + 1 (for getCallerFunctionName) = 2.
    // If getCallerFunctionName is called by an intermediate helper within the function we want to identify,
    // you might need to adjust the skip count.

    // Let's assume the function that *uses* this utility is the one we want.
    // So, if MyFunction calls thisUtility which calls getCallerFunctionName,
    // and we want "MyFunction":
    // runtime.Caller(0) -> runtime.Caller
    // runtime.Caller(1) -> getCallerFunctionName
    // runtime.Caller(2) -> thisUtility
    // runtime.Caller(3) -> MyFunction
    // So, if this is a direct utility, skip=2 is common.

    pc, _, _, ok := runtime.Caller(skip) // Skip 1 frame (runtime.Caller itself) to get the immediate caller.
                                      // If this function is a helper, and you want the caller of the helper,
                                      // you'd use runtime.Caller(2).
    if !ok {
        return "unknown"
    }

    fn := runtime.FuncForPC(pc)
    if fn == nil {
        return "unknown"
    }
    // The name returned by fn.Name() is often fully qualified (e.g., "main.MyFunction" or "github.com/user/project/package.MyFunction")
    // You might want to process it to get just the function name.
    name := fn.Name()
    // Example to get just the function part after the last dot.
    if lastDot := strings.LastIndex(name, "."); lastDot != -1 {
        name = name[lastDot+1:]
    }
    // Or after the last slash if it's a method on a type from another package
    if lastSlash := strings.LastIndex(name, "/"); lastSlash != -1 && strings.Contains(name[lastSlash:], ".") {
        // More complex parsing might be needed for package paths vs. method receivers
    }
    return name
}