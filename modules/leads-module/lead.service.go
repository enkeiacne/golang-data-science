package leads_module

import (
	"archive/tar"
	"compress/gzip"
	"encoding/base64"
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
)

func Run() error {
	var leadFile entities.LeadFileHistory
	if err := database.DB.Where("file_name = ?", "files-20230730.csv.tar.gz").First(&leadFile).Error; err != nil {
		return err
	}
	//if err := downloadAnSplitFile(leadFile, 3); err != nil {
	//	return err
	//}
	//if err := mergeFileDownloaded(leadFile.FileName, 3); err != nil {
	//	return err
	//}
	//result, err := extractTarGz("storage/tmp/files-20230730.csv.tar.gz", "storage/tmp")
	//if err != nil {
	//	log.Println(err)
	//}
	//fmt.Println(result)
	readCsv("storage/tmp/files-20230730.csv")
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
func readCsv(filepath string) {
	// Open the CSV file
	f, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	// Read the CSV file into a Gota DataFrame
	df := dataframe.ReadCSV(f)

	// Display the DataFrame (optional)
	fmt.Println(df)
	// Group by the phone number column and count occurrences
	groupByPhone := df.GroupBy("phone")

	// Filter to get only duplicates (count > 1)

	// Display duplicate phone numbers and their counts
	fmt.Println("Duplicate phone numbers and their counts:")
	fmt.Println(groupByPhone)
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

// section import csv to database
//const (
//	batchSize  = 1000
//	numWorkers = 10000 // Increase the number of workers
//)
//
//// import v1
//func importLead(filepath string) error {
//	file, err := os.Open(filepath)
//	if err != nil {
//		return fmt.Errorf("unable to read input file %s: %v", filepath, err)
//	}
//	defer file.Close()
//
//	reader := csv.NewReader(file)
//	records, err := reader.ReadAll()
//	if err != nil {
//		return fmt.Errorf("unable to parse file as CSV for %s: %v", filepath, err)
//	}
//
//	const numWorkers = 10000
//	var wg sync.WaitGroup
//	recordCh := make(chan []string, len(records))
//
//	// Start worker goroutines
//	for i := 0; i < numWorkers; i++ {
//		wg.Add(1)
//		go worker(&wg, recordCh)
//	}
//
//	// Send records to the worker goroutines
//	for _, record := range records {
//		recordCh <- record
//	}
//	close(recordCh)
//
//	// Wait for all workers to finish
//	wg.Wait()
//
//	return nil
//}
//func worker(wg *sync.WaitGroup, recordCh chan []string) {
//	defer wg.Done()
//	var batch []entities.LeadDomainRelations
//	for record := range recordCh {
//		if leadDomainRelation, err := processRecord(record); err == nil {
//			batch = append(batch, leadDomainRelation)
//			if len(batch) >= batchSize {
//				if err := insertBatch(batch); err != nil {
//					fmt.Printf("error inserting batch: %v\n", err)
//				}
//				batch = batch[:0]
//			}
//		} else {
//			fmt.Printf("error processing record %v: %v\n", record, err)
//		}
//	}
//	if len(batch) > 0 {
//		if err := insertBatch(batch); err != nil {
//			fmt.Printf("error inserting final batch: %v\n", err)
//		}
//	}
//}
//func processRecord(record []string) (entities.LeadDomainRelations, error) {
//	var leadDomain entities.LeadDomains
//	if err := database.DB.Where("name = ?", record[1]).First(&leadDomain).Error; err != nil {
//		if errors.Is(err, gorm.ErrRecordNotFound) {
//			leadDomain = entities.LeadDomains{Name: record[1]}
//			if err := database.DB.Create(&leadDomain).Error; err != nil {
//				return entities.LeadDomainRelations{}, err
//			}
//		} else {
//			return entities.LeadDomainRelations{}, err
//		}
//	}
//
//	var lead entities.Leads
//	if err := database.DB.Where("phone = ?", record[0]).First(&lead).Error; err != nil {
//		if errors.Is(err, gorm.ErrRecordNotFound) {
//			lead = entities.Leads{Phone: record[0]}
//			if err := database.DB.Create(&lead).Error; err != nil {
//				return entities.LeadDomainRelations{}, err
//			}
//		} else {
//			return entities.LeadDomainRelations{}, err
//		}
//	}
//
//	leadDomainRelation := entities.LeadDomainRelations{
//		LeadId:       lead.ID,
//		LeadDomainId: leadDomain.ID,
//	}
//	return leadDomainRelation, nil
//}
//func insertBatch(batch []entities.LeadDomainRelations) error {
//	return database.DB.Create(&batch).Error
//}

// import v2
//func importLeadBuffer(filepath string) error {
//	file, err := os.Open(filepath)
//	if err != nil {
//		return fmt.Errorf("unable to read input file %s: %v", filepath, err)
//	}
//	defer file.Close()
//
//	reader := csv.NewReader(bufio.NewReader(file))
//	var wg sync.WaitGroup
//	recordChan := make(chan []string, batchSize)
//
//	// Goroutine to read records and send to channel
//	go func() {
//		defer close(recordChan)
//		for {
//			record, err := reader.Read()
//			if err != nil {
//				if err.Error() == "EOF" {
//					break
//				}
//				fmt.Println("Error reading record:", err)
//				continue
//			}
//			recordChan <- record
//		}
//	}()
//
//	// Goroutine pool to process records concurrently
//	for i := 0; i < numWorkers; i++ {
//		wg.Add(1)
//		go func() {
//			defer wg.Done()
//			batch := make([][]string, 0, batchSize)
//			for record := range recordChan {
//				batch = append(batch, record)
//				if len(batch) >= batchSize {
//					if err := saveBatch(batch); err != nil {
//						fmt.Println("Error saving batch:", err)
//					}
//					batch = batch[:0]
//				}
//			}
//			// Save remaining records
//			if len(batch) > 0 {
//				if err := saveBatch(batch); err != nil {
//					fmt.Println("Error saving batch:", err)
//				}
//			}
//		}()
//	}
//
//	wg.Wait()
//	return nil
//}
//func saveBatch(batch [][]string) error {
//	log.Println("save migrating")
//	return database.DB.Transaction(func(tx *gorm.DB) error {
//		leadDomains := make(map[string]entities.LeadDomains)
//		leads := make(map[string]entities.Leads)
//
//		// Collect unique lead domains and leads
//		for _, record := range batch {
//			if _, exists := leadDomains[record[1]]; !exists {
//				leadDomains[record[1]] = entities.LeadDomains{Name: record[1]}
//			}
//			if _, exists := leads[record[0]]; !exists {
//				leads[record[0]] = entities.Leads{Phone: record[0]}
//			}
//		}
//
//		// Bulk insert lead domains
//		if len(leadDomains) > 0 {
//			leadDomainsList := make([]entities.LeadDomains, 0, len(leadDomains))
//			for _, ld := range leadDomains {
//				leadDomainsList = append(leadDomainsList, ld)
//			}
//			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&leadDomainsList).Error; err != nil {
//				return err
//			}
//		}
//
//		// Bulk insert leads
//		if len(leads) > 0 {
//			leadsList := make([]entities.Leads, 0, len(leads))
//			for _, l := range leads {
//				leadsList = append(leadsList, l)
//			}
//			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&leadsList).Error; err != nil {
//				return err
//			}
//		}
//
//		// Create lead domain relations
//		for _, record := range batch {
//			var leadDomain entities.LeadDomains
//			if err := tx.Where("name = ?", record[1]).First(&leadDomain).Error; err != nil {
//				return err
//			}
//
//			var lead entities.Leads
//			if err := tx.Where("phone = ?", record[0]).First(&lead).Error; err != nil {
//				return err
//			}
//
//			leadDomainRelation := entities.LeadDomainRelations{
//				LeadId:       lead.ID,
//				LeadDomainId: leadDomain.ID,
//			}
//			if err := tx.Create(&leadDomainRelation).Error; err != nil {
//				return err
//			}
//		}
//		return nil
//	})
//}
