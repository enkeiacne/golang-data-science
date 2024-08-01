package leads_module

import (
	"archive/tar"
	"compress/gzip"
	"encoding/base64"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/go-gota/gota/dataframe"
	"import/commons/enums"
	"import/configs"
	"import/database"
	"import/database/entities"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func Run() error {
	var leadFile entities.LeadFileHistory
	if err := database.DB.Where("file_name = ?", "files-20230730.csv.tar.gz").First(&leadFile).Error; err != nil {
		return err
	}
	if err := downloadAnSplitFile(leadFile, 3); err != nil {
		return err
	}
	if err := mergeFileDownloaded(leadFile.FileName, 3); err != nil {
		return err
	}
	result, err := extractTarGz("storage/tmp/files-20230730.csv.tar.gz", "storage/tmp")
	if err != nil {
		log.Println(err)
	}
	resultCsv, err := readCsv(result[0], leadFile.FileName)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 1) // Buffered channel to handle error propagation

	// Run importLeadPhone
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := importLeadPhone((*resultCsv)[0], leadFile.FileName, 10000, 10000); err != nil {
			errCh <- fmt.Errorf("error Phone Import: %v", err)
		}
	}()

	// Run importLeadDomain
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := importLeadDomain((*resultCsv)[1], leadFile.FileName, 10000, 10000); err != nil {
			errCh <- fmt.Errorf("error domain import: %v", err)
		}
	}()

	// Run importLeadPhoneDuplicate
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := importLeadPhoneDuplicate((*resultCsv)[0], leadFile.FileName, 10000, 10000); err != nil {
			errCh <- fmt.Errorf("error: %v", err)
		} else {
			fmt.Printf("Data import phone count duplicate completed\n")
		}
	}()

	// Wait for all goroutines to finish
	go func() {
		wg.Wait()
		close(errCh) // Close the channel once all goroutines are done
	}()

	// Check if there were any errors
	for err := range errCh {
		fmt.Println(err)
		return err // Exit if any error is encountered
	}
	if err = importLeadDomainRelations(result[0], leadFile.FileName, 10000, 30000); err != nil {
		return err
	}
	cleanupFolder("storage/tmp/")
	fmt.Println("All imports completed successfully")
	return nil
}
func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}
func downloadAnSplitFile(leadFile entities.LeadFileHistory, numChunks int) error {
	client := &http.Client{}
	req, err := http.NewRequest("GET", configs.FileServerUrl+"/"+leadFile.FileName, nil)
	fmt.Println(req)
	if err != nil {
		updateFileHistoryStatus(leadFile.ID, enums.DOWNLOAD_FAILED)
		return err
	}

	// Add Basic Auth header
	req.Header.Add("Authorization", "Basic "+basicAuth(configs.FileServerUrlUsername, configs.FileServerUrlPassword))
	resp, err := client.Do(req)
	if err != nil {
		updateFileHistoryStatus(leadFile.ID, enums.DOWNLOAD_FAILED)
		log.Println(err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		updateFileHistoryStatus(leadFile.ID, enums.DOWNLOAD_FAILED)
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	// Create the directory if it doesn't exist
	if err := os.MkdirAll("storage/tmp", os.ModePerm); err != nil {
		updateFileHistoryStatus(leadFile.ID, enums.DOWNLOAD_FAILED)
		return err
	}

	// Calculate chunk size
	contentLength := resp.ContentLength
	if contentLength <= 0 {
		updateFileHistoryStatus(leadFile.ID, enums.DOWNLOAD_FAILED)
		return errors.New("invalid content length")
	}
	chunkSize := contentLength / int64(numChunks)
	fmt.Println("Content Length: ", contentLength)
	fmt.Println("Chunk Size: ", chunkSize)

	// Split and save to multiple files
	for i := 0; i < numChunks; i++ {
		chunkFilePath := fmt.Sprintf("storage/tmp/%s.part%d", leadFile.FileName, i+1)
		out, err := os.Create(chunkFilePath)
		if err != nil {
			updateFileHistoryStatus(leadFile.ID, enums.DOWNLOAD_FAILED)
			return err
		}

		written, err := io.CopyN(out, resp.Body, chunkSize)
		out.Close()
		if err != nil && err != io.EOF {
			updateFileHistoryStatus(leadFile.ID, enums.DOWNLOAD_FAILED)
			return err
		}

		// For the last chunk, write the remaining bytes
		if i == numChunks-1 && written < chunkSize {
			out, err := os.OpenFile(chunkFilePath, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
			if err != nil {
				updateFileHistoryStatus(leadFile.ID, enums.DOWNLOAD_FAILED)
				return err
			}
			_, err = io.Copy(out, resp.Body)
			out.Close()
			if err != nil {
				updateFileHistoryStatus(leadFile.ID, enums.DOWNLOAD_FAILED)
				return err
			}
		}
	}

	return nil
}
func mergeFileDownloaded(originalFileName string, numChunks int) error {
	// Dapatkan nama file tanpa direktori
	fileNameOnly := filepath.Base(originalFileName)

	// Tentukan jalur file gabungan
	mergedFilePath := filepath.Join("storage/tmp/", fileNameOnly)
	mergedFile, err := os.Create(mergedFilePath)
	if err != nil {
		return err
	}
	defer mergedFile.Close()

	// Gabungkan bagian-bagian file
	for i := 0; i < numChunks; i++ {
		chunkFilePath := fmt.Sprintf("storage/tmp/%s.part%d", fileNameOnly, i+1)
		chunkFile, err := os.Open(chunkFilePath)
		if err != nil {
			return err
		}

		if _, err := io.Copy(mergedFile, chunkFile); err != nil {
			chunkFile.Close()
			return err
		}
		chunkFile.Close()
	}

	return nil
}
func updateFileHistoryStatus(id uint, status string) {
	database.DB.Model(&entities.LeadFileHistory{}).Where("id = ?", id).Update("status", status)
}

func cleanupFiles(files ...string) {
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			log.Printf("Error removing file %s: %v\n", file, err)
		}
	}
}
func cleanupFolder(folderPath string) {
	if err := os.RemoveAll(folderPath); err != nil {
		log.Printf("failed to remove folder: %w", err)
	}
}
func extractTarGz(tarGzFile, targetDir string) ([]string, error) {
	var extractedFiles []string

	// Ensure target directory exists
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create target directory: %w", err)
	}

	f, err := os.Open(tarGzFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open tar.gz file: %w", err)
	}
	defer f.Close()

	gzf, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzf.Close()

	tarReader := tar.NewReader(gzf)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read tar header: %w", err)
		}

		// Sanitize and create target file path
		targetFile := filepath.Join(targetDir, filepath.Clean(header.Name))

		// Create directory for the target file
		if header.Typeflag == tar.TypeDir {
			if err := os.MkdirAll(targetFile, 0755); err != nil {
				return nil, fmt.Errorf("failed to create directory: %w", err)
			}
		} else if header.Typeflag == tar.TypeReg {
			// Ensure parent directories exist
			dir := filepath.Dir(targetFile)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create directory: %w", err)
			}

			// Create file
			file, err := os.OpenFile(targetFile, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return nil, fmt.Errorf("failed to create file: %w", err)
			}
			defer file.Close()

			// Copy file contents
			if _, err := io.Copy(file, tarReader); err != nil {
				return nil, fmt.Errorf("failed to copy file contents: %w", err)
			}
			extractedFiles = append(extractedFiles, targetFile)
		} else {
			log.Printf("Unsupported type: %v in %s", header.Typeflag, header.Name)
		}
	}

	return extractedFiles, nil
}
func readCsv(filepathCsv string, fileName string) (*[]string, error) {
	// Open the CSV file
	f, err := os.Open(filepathCsv)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	// Read the CSV file into a Gota DataFrame
	df := dataframe.ReadCSV(f)
	//Maps to count occurrences
	phoneCount := make(map[string]int)
	domainCount := make(map[string]int)

	// Iterate through the DataFrame rows
	for i := 0; i < df.Nrow(); i++ {
		phone := df.Elem(i, 0).String()
		domain := df.Elem(i, 1).String()

		// Count phone numbers
		phoneCount[phone]++

		// Count domains
		domainCount[domain]++
	}
	dir := "storage/tmp"
	phoneFilePath := filepath.Join(dir, fileName+"_phone_count.csv")
	phoneFile, err := os.Create(phoneFilePath)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return nil, err
	}
	defer phoneFile.Close()

	phoneWriter := csv.NewWriter(phoneFile)
	defer phoneWriter.Flush()

	phoneWriter.Write([]string{"phone", "count_duplicate"})
	for phone, count := range phoneCount {
		err := phoneWriter.Write([]string{phone, fmt.Sprintf("%d", count)})
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return nil, err
		}
	}

	// Write domain counts to a new CSV file
	domainFilePath := filepath.Join(dir, fileName+"_domain_count.csv")
	domainFile, err := os.Create(domainFilePath)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return nil, err
	}
	defer domainFile.Close()

	domainWriter := csv.NewWriter(domainFile)
	defer domainWriter.Flush()

	domainWriter.Write([]string{"domain", "count_duplicate"})
	for domain, count := range domainCount {
		if count > 1 {
			count-- // subtract 1 to reflect the count of duplicates
		} else {
			count = 0 // if no duplicates, set to 0
		}
		err := domainWriter.Write([]string{domain, fmt.Sprintf("%d", count)})
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return nil, err
		}
	}
	result := []string{phoneFilePath, domainFilePath}
	return &result, nil
}
func importLeadPhone(filepathPhoneCSV string, fileName string, chunkSize int, numWorkers int) error {
	// Open the CSV file
	file, err := os.Open(filepathPhoneCSV)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	createdAt := ExtractDateFromFilename(fileName)
	// Read CSV data
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("error reading CSV file: %v", err)
	}

	// Prepare data for insertion
	var data []entities.Lead
	phoneSet := make(map[string]struct{}) // To keep track of phones to avoid duplicates

	for _, record := range records {
		phone := record[0]
		if _, exists := phoneSet[phone]; !exists {
			phoneSet[phone] = struct{}{}
			data = append(data, entities.Lead{
				Phone:     phone,
				CreatedAt: createdAt,
			})
		}
	}

	// Channel for sending data to workers
	dataChan := make(chan []entities.Lead, numWorkers)
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range dataChan {
				tx := database.DB.Begin()
				if err := tx.Create(&chunk).Error; err != nil {
					fmt.Println("Error inserting records:", err)
					tx.Rollback()
				} else {
					tx.Commit()
				}
			}
		}()
	}

	// Send data to workers in chunks
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		dataChan <- data[i:end]
	}

	// Close the channel and wait for workers to finish
	close(dataChan)
	wg.Wait()

	fmt.Println("Data imported successfully.")
	return nil
}
func importLeadDomain(filepathDomainCSV string, fileName string, chunkSize int, numWorkers int) error {
	// Open the CSV file
	file, err := os.Open(filepathDomainCSV)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	createdAt := ExtractDateFromFilename(fileName)
	// Read CSV data
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("error reading CSV file: %v", err)
	}

	// Prepare data for insertion
	var data []entities.LeadDomain
	domainSet := make(map[string]struct{}) // To keep track of phones to avoid duplicates

	for _, record := range records {
		domain := record[0]
		if _, exists := domainSet[domain]; !exists {
			domainSet[domain] = struct{}{}
			data = append(data, entities.LeadDomain{
				Name:      domain,
				CreatedAt: createdAt,
			})
		}
	}

	// Channel for sending data to workers
	dataChan := make(chan []entities.LeadDomain, numWorkers)
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range dataChan {
				tx := database.DB.Begin()
				if err := tx.Create(&chunk).Error; err != nil {
					fmt.Println("Error inserting records:", err)
					tx.Rollback()
				} else {
					tx.Commit()
				}
			}
		}()
	}

	// Send data to workers in chunks
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		dataChan <- data[i:end]
	}

	// Close the channel and wait for workers to finish
	close(dataChan)
	wg.Wait()

	fmt.Println("Data imported successfully.")
	return nil
}
func importLeadPhoneDuplicate(filepathPhoneCSV string, fileName string, chunkSize int, numWorkers int) error {
	// Open the CSV file
	file, err := os.Open(filepathPhoneCSV)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	createdAt := ExtractDateFromFilename(fileName)
	// Read CSV data
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("error reading CSV file: %v", err)
	}
	log.Println("importing")
	// Map to count occurrences
	phoneCount := make(map[string]int)
	for _, record := range records {
		phone := record[0]
		phoneCount[phone]++
	}

	// Prepare the data to be inserted
	var data []entities.LeadPhoneDuplicateHistory
	for phone, count := range phoneCount {
		data = append(data, entities.LeadPhoneDuplicateHistory{
			Phone:          phone,
			DuplicateCount: count,
			CreatedAt:      createdAt,
			UpdatedAt:      createdAt,
		})
	}

	// Channel for sending data to workers
	dataChan := make(chan []entities.LeadPhoneDuplicateHistory, numWorkers)

	// WaitGroup to wait for all workers to finish
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range dataChan {
				if err := database.DB.Create(&chunk).Error; err != nil {
					fmt.Println("Error inserting records:", err)
				}
			}
		}()
	}

	// Send data to workers in chunks
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		dataChan <- data[i:end]
	}

	// Close the channel and wait for workers to finish
	close(dataChan)
	wg.Wait()

	return nil
}

func importLeadDomainRelations(filepathMainCsv string, fileName string, chunkSize int, numWorkers int) error {
	// Open the CSV file
	file, err := os.Open(filepathMainCsv)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	createdAt := ExtractDateFromFilename(fileName)
	// Read CSV data
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("error reading CSV file: %v", err)
	}

	jobs := make(chan [][]string, numWorkers)
	results := make(chan error, numWorkers)
	var wg sync.WaitGroup

	// Worker pool
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range jobs {
				var leadDomainRelations []entities.LeadDomainRelations

				for _, record := range chunk {
					if len(record) < 2 {
						continue // Skip invalid records
					}

					leadDomainRelations = append(leadDomainRelations, entities.LeadDomainRelations{
						Phone:     record[0],
						Domain:    record[1],
						CreatedAt: createdAt,
						UpdatedAt: createdAt,
					})
				}

				if len(leadDomainRelations) > 0 {
					if err := database.DB.CreateInBatches(leadDomainRelations, chunkSize).Error; err != nil {
						results <- fmt.Errorf("error inserting data: %v", err)
						return
					}
				}
				results <- nil
			}
		}()
	}

	// Distribute the work to the workers
	for i := 0; i < len(records); i += chunkSize {
		end := i + chunkSize
		if end > len(records) {
			end = len(records)
		}

		jobs <- records[i:end]
	}
	close(jobs)

	// Wait for all workers to finish and check results
	go func() {
		wg.Wait()
		close(results)
	}()

	for err := range results {
		if err != nil {
			return err
		}
	}

	return nil
}

func ExtractDateFromFilename(filename string) time.Time {
	// Split the filename to extract the date part
	parts := strings.Split(filename, "-")
	if len(parts) < 2 {
		return time.Now()
	}

	dateStr := strings.Split(parts[1], ".")[0] // Extract '20230730'

	// Parse the date string into a time.Time object
	timestamp, err := time.Parse("20060102", dateStr)
	if err != nil {
		return time.Now()
	}
	return timestamp
}
