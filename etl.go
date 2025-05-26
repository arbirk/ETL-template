package main

import (
	"fmt"
	"time"

	"github.com/arbirk/ETL-template/tools"
)

// UserData defines the structure of our user records
type UserData struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// TransformedUserData defines the structure after transformation
type TransformedUserData struct {
	UserID        string `json:"userId"`
	ProcessedName string `json:"processedName"`
	Timestamp     string `json:"timestamp"`
}

//goetl:type=extract versioned next=TransformUserData
func ExtractUsers() error {
	fmt.Println("Extracting users...")
	time.Sleep(100 * time.Millisecond) // Simulate work

	encoder, closer, version, filePath, err := tools.GetNextVersionedJSONLWriter("extract", "ExtractUsers")
	if err != nil {
		// The error from GetNextVersionedJSONLWriter will already be descriptive.
		return err
	}
	defer closer()
	fmt.Printf("Extracting users to version %d at %s\n", version, filePath)

	users := []UserData{
		{ID: "4", Name: "Karen"},
		{ID: "5", Name: "Kevin"},
	}
	for _, user := range users {
		if err := encoder.Encode(user); err != nil {
			return fmt.Errorf("encoding user %+v to %s: %w", user, filePath, err)
		}
	}
	fmt.Printf("Users extracted successfully to %s (Version %d)\n", filePath, version)
	return nil
}

//goetl:type=transform temp_path=transformed_users.jsonl next=LoadProcessedUsers
func TransformUserData() error {
	fmt.Println("Transforming user data...")
	time.Sleep(150 * time.Millisecond)

	var extractedUsers []UserData
	_, err := tools.ReadLatestVersionedJSONL("extract", "ExtractUsers", &extractedUsers)
	if err != nil {
		return fmt.Errorf("reading latest extracted users .jsonl: %w", err)
	}
	fmt.Printf("Read %d users from ExtractUsers output for transformation.\n", len(extractedUsers))

	tempOutputFileName := "transformed_users.jsonl"
	tempFilePath, err := tools.GetTempFilePath("TransformUserData", tempOutputFileName)
	if err != nil {
		return fmt.Errorf("getting temp file path for TransformUserData: %w", err)
	}
	fmt.Printf("Transformed data will be written to temporary file: %s\n", tempFilePath)

	encoder, closer, err := tools.NewJSONLWriter(tempFilePath)
	if err != nil {
		return fmt.Errorf("creating JSONL writer for temp file %s: %w", tempFilePath, err)
	}
	defer closer()

	var transformedOutputUsers []TransformedUserData
	for _, user := range extractedUsers {
		transformed := TransformedUserData{
			UserID:        user.ID,
			ProcessedName: fmt.Sprintf("Processed_%s", user.Name),
			Timestamp:     time.Now().Format(time.RFC3339Nano),
		}
		if err := encoder.Encode(transformed); err != nil {
			return fmt.Errorf("encoding transformed user %+v: %w", transformed, err)
		}
		transformedOutputUsers = append(transformedOutputUsers, transformed)
	}

	fmt.Printf("Transformed %d users to %s\n", len(transformedOutputUsers), tempFilePath)
	return nil
}

//goetl:type=load versioned
func LoadProcessedUsers() error {
	fmt.Println("Loading processed users...")
	time.Sleep(200 * time.Millisecond)

	tempInputFileName := "transformed_users.jsonl"
	tempFilePath, err := tools.GetTempFilePath("TransformUserData", tempInputFileName)
	if err != nil {
		return fmt.Errorf("getting temp file path for reading transformed data: %w", err)
	}

	var usersToLoad []TransformedUserData
	err = tools.ReadAllJSONLFile(tempFilePath, &usersToLoad)
	if err != nil {
		return fmt.Errorf("reading transformed users from temp file %s: %w", tempFilePath, err)
	}
	fmt.Printf("Read %d users from %s to load.\n", len(usersToLoad), tempFilePath)


	loadReportPath, version, err := tools.GetNextVersionedFilePath("load", "LoadProcessedUsers")
	if err != nil {
		return fmt.Errorf("getting next versioned .jsonl file path for LoadProcessedUsers report: %w", err)
	}
	fmt.Printf("Load report/manifest for version %d will be at %s\n", version, loadReportPath)

	loadSummary := struct {
		LoadedCount int    `json:"loadedCount"`
		ReportPath  string `json:"reportPath"`
		LoadTime    string `json:"loadTime"`
	}{
		LoadedCount: len(usersToLoad),
		ReportPath:  loadReportPath,
		LoadTime:    time.Now().Format(time.RFC3339Nano),
	}

	encoder, closer, err := tools.NewJSONLWriter(loadReportPath)
	if err != nil {
		return fmt.Errorf("creating JSONL writer for load summary %s: %w", loadReportPath, err)
	}
	defer closer()

	if err := encoder.Encode(loadSummary); err != nil {
		return fmt.Errorf("encoding load summary to %s: %w", loadReportPath, err)
	}

	fmt.Printf("Load process completed. Summary saved to %s (Version %d)\n", loadReportPath, version)
	return nil
}

func main() {
	tools.Knoll()

	finalStatusFile := "output/status/etl_run_status.json"
	run := tools.NewPipelineRun(finalStatusFile)
	run.LogStatus()

	run.ExecuteStep("ExtractUsers", ExtractUsers)
	run.ExecuteStep("TransformUserData", TransformUserData)
	run.ExecuteStep("LoadProcessedUsers", LoadProcessedUsers)

	run.Stow()
}
