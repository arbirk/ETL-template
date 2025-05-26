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

	encoder, closer, version, filePath, err := tools.GetNextVersionedJSONLWriter()
	defer closer()
    if err != nil {return err}
	fmt.Printf("Extracting users to version %d at %s\n", version, filePath)

	users := []UserData{
		{ID: "4", Name: "Karen"},
		{ID: "5", Name: "Kevin"},
        {ID: "6", Name: "Steven"},
        {ID: "7", Name: "Laura"},
        {ID: "8", Name: "Nina"},
        {ID: "9", Name: "Oscar"},
        {ID: "10", Name: "Paul"},
        {ID: "11", Name: "Quinn"},
        {ID: "12", Name: "Rachel"},

	}
	for _, user := range users {
		if err := encoder.Encode(user); err != nil {
			return fmt.Errorf("encoding user %+v to %s: %w", user, filePath, err)
		}
	}

	err = LoadProcessedUsers(TransformUsers(users))

	if err != nil {return err}
	return nil
}
//goetl:type=transform temp_path=transformed_users.jsonl next=LoadProcessedUsers
func TransformUsers(users []UserData) []TransformedUserData {
    var transformedOutputUsers []TransformedUserData
    for _, user := range users {
        transformed := TransformedUserData{
            UserID:        user.ID,
            ProcessedName: fmt.Sprintf("Processed_%s", user.Name),
            Timestamp:     time.Now().Format(time.RFC3339Nano),
        }
        transformedOutputUsers = append(transformedOutputUsers, transformed)
    }
    return transformedOutputUsers
}

//goetl:type=load versioned
func LoadProcessedUsers(tusers []TransformedUserData) error {
	fmt.Println("Loading processed users...")

    encoder, closer, version, filePath, err := tools.GetNextVersionedJSONLWriter()
	defer closer()
    if err != nil {return err}
	fmt.Printf("Extracting users to version %d at %s\n", version, filePath)

	loadSummary := struct {
		LoadedCount int    `json:"loadedCount"`
		ReportPath  string `json:"reportPath"`
		LoadTime    string `json:"loadTime"`
	}{
		LoadedCount: len(tusers),
		ReportPath:  filePath,
		LoadTime:    time.Now().Format(time.RFC3339Nano),
	}

	if err := encoder.Encode(loadSummary); err != nil {
		return fmt.Errorf("encoding load summary to %s: %w", filePath, err)
	}

	fmt.Printf("Load process completed. Summary saved to %s (Version %d)\n", filePath, version)
	return nil
}

func main() {
	tools.Knoll()

	finalStatusFile := "output/status/etl_run_status.json"
	run := tools.NewPipelineRun(finalStatusFile)
	run.LogStatus()

	run.ExecuteStep("ExtractUsers", ExtractUsers)

	run.Stow()
}
