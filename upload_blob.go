package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
)

// Simulate file generation with in-memory data
func generateInMemoryFile(size int64) *os.File {
	file, err := os.CreateTemp("", "tempfile_*.bin") // Create a temporary file
	if err != nil {
		log.Fatalf("Failed to create temporary file: %v", err)
	}

	buffer := make([]byte, 1024*1024) // 1MB buffer
	for written := int64(0); written < size; written += int64(len(buffer)) {
		if _, err := rand.Read(buffer); err != nil {
			log.Fatalf("Failed to generate random data: %v", err)
		}
		if _, err := file.Write(buffer); err != nil {
			log.Fatalf("Failed to write to temporary file: %v", err)
		}
	}

	file.Seek(0, 0) // Reset the file pointer for reading
	return file
}

func main() {
	// Define command-line flags
	accountName := flag.String("accountName", "", "Azure Storage account name")
	accountKey := flag.String("accountKey", "", "Azure Storage account key")
	containerName := flag.String("containerName", "", "Azure Blob Storage container name")
	numFiles := flag.Int("numFiles", 1000, "Number of files to upload")
	fileSize := flag.Int64("fileSize", 64, "Size of each file in MB")
	numWorkers := flag.Int("workers", 8, "Number of concurrent workers")
	concurrency := flag.Int("concurrency", 16, "Concurrency level for each worker")

	// Parse the flags
	flag.Parse()

	// Validate required flags
	if *accountName == "" || *accountKey == "" || *containerName == "" {
		log.Fatalf("Error: accountName, accountKey, and containerName are required flags.")
	}

	// Create a shared key credential
	credential, err := azblob.NewSharedKeyCredential(*accountName, *accountKey)
	if err != nil {
		log.Fatalf("Failed to create shared key credential: %v", err)
	}

	// Create a client for the Azure Blob Storage account
	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", *accountName)
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, credential, nil)
	if err != nil {
		log.Fatalf("Failed to create Azure Blob Storage client: %v", err)
	}

	// Get a container client
	containerClient := client.ServiceClient().NewContainerClient(*containerName)

	// Step 1: Simulate file generation
	tempFile := generateInMemoryFile(*fileSize * 1024 * 1024) // Convert MB to bytes
	defer os.Remove(tempFile.Name())                          // Clean up the temporary file

	// Step 2: Upload all files in parallel using a worker pool and measure time
	jobs := make(chan int, *numFiles)
	results := make(chan error, *numFiles)

	startTime := time.Now() // Start timing

	// Worker function
	worker := func() {
		for i := range jobs {
			fileName := fmt.Sprintf("file_%d.bin", i)
			blobClient := containerClient.NewBlockBlobClient(fileName)

			// Open the temporary file for each upload
			file, err := os.Open(tempFile.Name())
			if err != nil {
				results <- fmt.Errorf("failed to open temporary file for file %s: %v", fileName, err)
				continue
			}

			_, err = blobClient.UploadStream(context.Background(), file, &blockblob.UploadStreamOptions{
				Concurrency: *concurrency, // Use user-defined concurrency
			})
			file.Close()
			if err != nil {
				results <- fmt.Errorf("failed to upload file %s: %v", fileName, err)
				continue
			}

			fmt.Printf("File %s uploaded successfully!\n", fileName)
			results <- nil
		}
	}

	// Start workers
	for i := 0; i < *numWorkers; i++ {
		go worker()
	}

	// Send jobs to workers
	for i := 1; i <= *numFiles; i++ {
		jobs <- i
	}
	close(jobs)

	// Wait for all results
	for range jobs {
		if err := <-results; err != nil {
			log.Println(err)
		}
	}

	elapsedTime := time.Since(startTime) // End timing

	// Calculate throughput
	totalData := float64(*numFiles) * float64(*fileSize) // Total data in MB
	throughput := totalData / elapsedTime.Seconds()      // Throughput in MB/s

	fmt.Printf("Total upload time: %.2f seconds\n", elapsedTime.Seconds())
	fmt.Printf("Throughput: %.2f MB/s\n", throughput)
}
