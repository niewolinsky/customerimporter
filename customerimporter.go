// Package customerimporter provides functions for reading customer data from CSV file
// and counting unique email domains of customers.
package customerimporter

import (
	"encoding/csv"
	"fmt"
	"io"
	"net"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
)

// Const "CSV_FIRST_LINE_NUMBER" signifies first line of an open CSV file.
const CSV_FIRST_LINE_NUMBER = 1

// Const "MIN_CHUNK_SIZE" signifies the minimum size for a chunk
const MIN_CHUNK_SIZE = 1

// Function "isHeaderLine" checks for CSV header repetition in a single CSV file.
// Could also be a generic function to compare two string slices.
func isHeaderLine(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// Type "email" provides simple utilties for working with email addresses.
type email string

// Variable "emailRegex" is precompiled regex that checks for email correctness.
var emailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)

// Method "isValid" checks for email correctness using precompiled regex value "emailRegex".
func (e email) isValid() bool {
	return emailRegex.MatchString(string(e))
}

// Method "extractDomain" extracts the domain part from an email address.
// It assumes the email address is valid.
func (e email) extractDomain() string {
	parts := strings.Split(string(e), "@")
	return parts[1]
}

// Type "gender" contains all valid genders as enum value.
type gender int

const (
	unknown gender = iota
	male
	female
	transgender
	// and more...
)

// Function "parseGender" checks whether "gender" value is on the list of valid genders, otherwise returns "unknown" as value.
func parseGender(genderStr string) gender {
	var genderMap = map[string]gender{
		"male":        male,
		"female":      female,
		"transgender": transgender,
	}

	genderStr = strings.ToLower(genderStr)
	val, exists := genderMap[genderStr]
	if exists {
		return val
	}

	return unknown
}

// Type "customer" reflects the expected structure of a customer data in CSV file.
type customer struct {
	FirstName string
	LastName  string
	Email     email
	Gender    gender
	IPAddress net.IP
}

// Interface "DomainProvider" is for types that can provide a domain string.
type DomainProvider interface {
	GetDomain() string
}

func (c customer) GetDomain() string {
	return c.Email.extractDomain()
}

// Type "domainCount" groups domain name and its occurences in a CSV file in a single struct.
type domainCount struct {
	Domain string
	Count  int
}

// Function "sortDomainCounts" translates a map of domains and its occurences to a "domainCount" slice and
// sorts it by the count.
func sortDomainCounts(domainCounts map[string]int) []domainCount {
	var domainCountSlice []domainCount

	for domain, count := range domainCounts {
		domainCountSlice = append(domainCountSlice, domainCount{Domain: domain, Count: count})
	}

	sort.Slice(domainCountSlice, func(i, j int) bool {
		return domainCountSlice[i].Count > domainCountSlice[j].Count
	})

	return domainCountSlice
}

// Function "CountDomains" returns a sorted slice of "domainCount" type, with unique domain names and their respective count.
func CountDomains(providers []DomainProvider) []domainCount {
	domainCounts := make(map[string]int)

	for _, provider := range providers {
		domain := provider.GetDomain()
		domainCounts[domain]++
	}

	return sortDomainCounts(domainCounts)
}

// Function "CountDomainsConcurrent" returns a sorted slice of "domainCount" type, with unique domain names and their respective count.
// It utilizes goroutines to speed up the process for larger datasets.
func CountDomainsConcurrent(providers []DomainProvider) []domainCount {
	domainCounts := make(map[string]int)

	// Optimize to machine
	numCores := runtime.NumCPU()
	totalProviders := len(providers)
	chunkSize := totalProviders / numCores

	if chunkSize < 1 {
		chunkSize = MIN_CHUNK_SIZE
	}

	var wg sync.WaitGroup
	mu := sync.Mutex{}

	processChunk := func(chunk []DomainProvider) {
		localCounts := make(map[string]int)
		for _, provider := range chunk {
			domain := provider.GetDomain()
			localCounts[domain]++
		}

		mu.Lock()
		for domain, count := range localCounts {
			domainCounts[domain] += count
		}
		mu.Unlock()
		wg.Done()
	}

	for i := 0; i < totalProviders; i += chunkSize {
		end := i + chunkSize
		if end > totalProviders {
			end = totalProviders
		}
		wg.Add(1)
		go processChunk(providers[i:end])
	}

	wg.Wait()

	return sortDomainCounts(domainCounts)
}

// Function "parseCustomerLine" maps single line from CSV file to "customer" struct. It returns an error if data is not valid.
func parseCustomerLine(csvLine []string, csvLineNumber int) (customer, error) {
	firstName := csvLine[0]
	if len(firstName) == 0 {
		return customer{}, fmt.Errorf("invalid first name at line %d: %s", csvLineNumber, csvLine[0])
	}

	lastName := csvLine[1]
	if len(lastName) == 0 {
		return customer{}, fmt.Errorf("invalid last name at line %d: %s", csvLineNumber, csvLine[1])
	}

	email := email(csvLine[2])
	if !email.isValid() {
		return customer{}, fmt.Errorf("invalid email at line %d: %s", csvLineNumber, csvLine[2])
	}

	gender := parseGender(csvLine[3])

	ipAddress := net.ParseIP(csvLine[4])
	if ipAddress == nil {
		return customer{}, fmt.Errorf("invalid ip address at line %d: %v", csvLineNumber, csvLine[4])
	}

	return customer{
		FirstName: csvLine[0],
		LastName:  csvLine[1],
		Email:     email,
		Gender:    gender,
		IPAddress: ipAddress,
	}, nil
}

// Type "ProcessCSVLineFunc" is used to abstract the processing logic when iterating over lines in a CSV file,
// allowing for different behaviors while reading and processing the CSV data.
type ProcessCSVLineFunc func([]string, int) error

// Function "ProcessCSVLine" processess a CSV file line by line, saving first line as CSV header.
// It accepts a callback satisfying "ProcessCSVLineFunc" type as second argument, modyfing behavior for what to do with read lines.
func ProcessCSVFile(csvReader *csv.Reader, processLine ProcessCSVLineFunc) error {
	csvLineNumber := CSV_FIRST_LINE_NUMBER

	//process first line as header
	csvHeader, err := csvReader.Read()
	if err != nil {
		return err
	}

	for {
		csvLine, err := csvReader.Read()
		csvLineNumber++
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error reading CSV at line %d: %w", csvLineNumber, err)
		}

		if isHeaderLine(csvLine, csvHeader) {
			continue
		}

		err = processLine(csvLine, csvLineNumber)
		if err != nil {
			return err
		}
	}

	return nil
}

// Function "ReadCustomersFromCSV" reads data from CSV file into a slice of "customer" type.
// It stores data in memory and should be avoided for larger datasets.
func ReadCustomersFromCSV(r io.Reader) ([]customer, error) {
	reader := csv.NewReader(r)

	var customers []customer

	err := ProcessCSVFile(reader, func(csvLine []string, csvLineNumber int) error {
		customer, err := parseCustomerLine(csvLine, csvLineNumber)
		if err != nil {
			return err
		}

		customers = append(customers, customer)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return customers, nil
}

// Function "ReadAndCountDomainsFromCSV" reads data from CSV file and processes it to return a count of each unique domain,
// sorted by their occurences. It does it by processing lines one by one and discarding them afterwards.
func ReadAndCountDomainsFromCSV(r io.Reader) ([]domainCount, error) {
	reader := csv.NewReader(r)

	domainCounts := make(map[string]int)

	err := ProcessCSVFile(reader, func(csvLine []string, csvLineNumber int) error {
		customer, err := parseCustomerLine(csvLine, csvLineNumber)
		if err != nil {
			return err
		}

		domain := email.extractDomain(customer.Email)
		domainCounts[domain]++
		return nil
	})
	if err != nil {
		return nil, err
	}

	sortedDomainCounts := sortDomainCounts(domainCounts)

	return sortedDomainCounts, nil
}
