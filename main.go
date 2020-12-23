package main

import (
	"context"
	"flag"
	"log"

	"github.com/dknowles2/gdrive_sync/gdrive"
	"github.com/dknowles2/gdrive_sync/uploader"
)

var (
	inputDir        = flag.String("input_dir", "/share/Scans", "Directory to watch for new files to upload")
	outputDir       = flag.String("output_dir", "Incoming Scans", "Drive folder where files should be uploaded")
	credsFile       = flag.String("creds_file", "/data/credentials.json", "credentials.json file")
	uploadOnStartup = flag.Bool("upload_on_startup", true, "When true, upload files in --input_dir on startup")
)

func main() {
	flag.Parse()
	ctx := context.Background()

	service, err := gdrive.New(ctx, *credsFile)
	if err != nil {
		log.Fatalf("Failed to create drive service: %s", err)
	}
	u, err := uploader.New(*inputDir, *outputDir, service)
	if err != nil {
		log.Fatalf("Failed to create Uploader: %v", err)
	}
	defer u.Close()
	if err := u.Run(ctx); err != nil {
		log.Fatalf("Run failed: %s", err)
	}
}
