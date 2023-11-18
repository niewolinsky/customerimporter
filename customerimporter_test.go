package customerimporter

import (
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
)

// Benchmark for the synchronous CountDomains function
func BenchmarkCountDomains(b *testing.B) {
	file, err := os.Open("../customers_1mil.csv")
	if err != nil {
		b.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	customers, err := ReadCustomersFromCSV(file)
	if err != nil {
		b.Fatalf("failed to read customers: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CountDomains(customers)
	}
}

// Benchmark for the concurrent CountDomains function
func BenchmarkCountDomainsConcurrent(b *testing.B) {
	file, err := os.Open("../customers_1mil.csv")
	if err != nil {
		b.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	customers, err := ReadCustomersFromCSV(file)
	if err != nil {
		b.Fatalf("failed to read customers: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CountDomainsConcurrent(customers)
	}
}

// Benchmark for the combined ReadAndCountDomainsFromCSV function
func BenchmarkReadAndCountDomainsFromCSV(b *testing.B) {
	for i := 0; i < b.N; i++ {
		file, err := os.Open("../customers_1mil.csv")
		if err != nil {
			b.Fatalf("failed to open file: %v", err)
		}

		_, err = ReadAndCountDomainsFromCSV(file)
		file.Close()
		if err != nil {
			b.Fatalf("failed to read and count domains: %v", err)
		}
	}
}

// Benchmark for the combined ReadCustomersFromCSV And CountDomains functions
func BenchmarkReadCustomersFromCSVAndCountDomains(b *testing.B) {
	for i := 0; i < b.N; i++ {
		file, err := os.Open("../customers_1mil.csv")
		if err != nil {
			b.Fatalf("Failed to open file: %v", err)
		}

		customers, err := ReadCustomersFromCSV(file)
		file.Close()
		if err != nil {
			b.Fatalf("Failed to read customers: %v", err)
		}

		_ = CountDomains(customers)
	}
}

// Benchmark for the combined ReadCustomersFromCSV And CountDomainsConcurrent functions
func BenchmarkReadCustomersFromCSVAndCountDomainsConcurrent(b *testing.B) {
	for i := 0; i < b.N; i++ {
		file, err := os.Open("../customers_1mil.csv")
		if err != nil {
			b.Fatalf("Failed to open file: %v", err)
		}

		customers, err := ReadCustomersFromCSV(file)
		file.Close()
		if err != nil {
			b.Fatalf("Failed to read customers: %v", err)
		}

		_ = CountDomainsConcurrent(customers)
	}
}

func TestIsHeaderLine(t *testing.T) {
	tests := []struct {
		name string
		a    []string
		b    []string
		want bool
	}{
		{
			name: "Identical slices",
			a:    []string{"a", "b", "c"},
			b:    []string{"a", "b", "c"},
			want: true,
		},
		{
			name: "Different slices of same length",
			a:    []string{"a", "b", "c"},
			b:    []string{"x", "y", "z"},
			want: false,
		},
		{
			name: "Slices of different lengths",
			a:    []string{"a", "b", "c"},
			b:    []string{"a", "b"},
			want: false,
		},
		{
			name: "Empty slices",
			a:    []string{},
			b:    []string{},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isHeaderLine(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("isHeaderLine(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestEmailIsValid(t *testing.T) {
	tests := []struct {
		name  string
		email email
		want  bool
	}{
		{
			name:  "Valid email",
			email: "test@example.com",
			want:  true,
		},
		{
			name:  "Invalid email without @",
			email: "testexample.com",
			want:  false,
		},
		{
			name:  "Invalid email without domain",
			email: "test@",
			want:  false,
		},
		{
			name:  "Invalid email with extra @",
			email: "test@@example.com",
			want:  false,
		},
		{
			name:  "Empty email",
			email: "",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.email.isValid()
			if got != tt.want {
				t.Errorf("email.isValid() for email %v = %v, want %v", tt.email, got, tt.want)
			}
		})
	}
}

func TestEmailExtractDomain(t *testing.T) {
	tests := []struct {
		name  string
		email email
		want  string
	}{
		{
			name:  "Extract domain from a standard email",
			email: "test@example.com",
			want:  "example.com",
		},
		{
			name:  "Email with subdomain",
			email: "test@sub.example.com",
			want:  "sub.example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.email.extractDomain()
			if got != tt.want {
				t.Errorf("email.extractDomain() for email %v = %v, want %v", tt.email, got, tt.want)
			}
		})
	}
}

func TestParseGender(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  gender
	}{
		{
			name:  "Male gender",
			input: "male",
			want:  male,
		},
		{
			name:  "Female gender",
			input: "female",
			want:  female,
		},
		{
			name:  "Transgender gender",
			input: "transgender",
			want:  transgender,
		},
		{
			name:  "Unspecified gender",
			input: "other",
			want:  unknown,
		},
		{
			name:  "Empty gender string",
			input: "",
			want:  unknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseGender(tt.input)
			if got != tt.want {
				t.Errorf("parseGender(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseCustomerLine(t *testing.T) {
	tests := []struct {
		name    string
		line    []string
		lineNum int
		want    customer
		wantErr bool
	}{
		{
			name:    "Valid line",
			line:    []string{"First", "Last", "first.last@example.com", "male", "192.168.1.1"},
			lineNum: 1,
			want: customer{
				FirstName: "First",
				LastName:  "Last",
				Email:     "first.last@example.com",
				Gender:    male,
				IPAddress: net.ParseIP("192.168.1.1"),
			},
			wantErr: false,
		},
		{
			name:    "Invalid email",
			line:    []string{"First", "Last", "first.last@@example.com", "male", "192.168.1.1"},
			lineNum: 2,
			wantErr: true,
		},
		{
			name:    "Invalid IP address",
			line:    []string{"First", "Last", "first.last@example.com", "male", "NOIP"},
			lineNum: 3,
			wantErr: true,
		},
		{
			name:    "Missing first name",
			line:    []string{"", "Last", "first.last@example.com", "male", "192.168.1.1"},
			lineNum: 4,
			wantErr: true,
		},
		{
			name:    "Missing last name",
			line:    []string{"First", "", "first.last@example.com", "male", "192.168.1.1"},
			lineNum: 4,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseCustomerLine(tt.line, tt.lineNum)

			if err != nil && !tt.wantErr {
				t.Fatalf("parseCustomerLine() unexpected error: %v", err)
				return
			}

			if err == nil && tt.wantErr {
				t.Errorf("parseCustomerLine() expected error, got none")
				return
			}

			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseCustomerLine() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCountDomains(t *testing.T) {
	tests := []struct {
		name      string
		customers []customer
		want      []domainCount
	}{
		{
			name: "Single domain",
			customers: []customer{
				{Email: "user1@example1.com"},
				{Email: "user2@example1.com"},
			},
			want: []domainCount{
				{Domain: "example1.com", Count: 2},
			},
		},
		{
			name: "Multiple domains",
			customers: []customer{
				{Email: "user1@example1.com"},
				{Email: "user2@example1.com"},
				{Email: "user3@example2.com"},
			},
			want: []domainCount{
				{Domain: "example1.com", Count: 2},
				{Domain: "example2.com", Count: 1},
			},
		},
		{
			name:      "No customers",
			customers: []customer{},
			want:      []domainCount{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CountDomains(tt.customers)

			//special case for no data
			if len(got) == 0 && len(tt.want) == 0 {
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CountDomains() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ! RUN TEST WITH RACE DETECTOR
func TestCountDomainsConcurrent(t *testing.T) {
	tests := []struct {
		name      string
		customers []customer
		want      []domainCount
	}{
		{
			name: "Single domain",
			customers: []customer{
				{Email: "user1@example1.com"},
				{Email: "user2@example1.com"},
			},
			want: []domainCount{
				{Domain: "example1.com", Count: 2},
			},
		},
		{
			name: "Multiple domains",
			customers: []customer{
				{Email: "user1@example1.com"},
				{Email: "user2@example2.com"},
				{Email: "user3@example1.com"},
			},
			want: []domainCount{
				{Domain: "example1.com", Count: 2},
				{Domain: "example2.com", Count: 1},
			},
		},
		{
			name:      "No customers",
			customers: []customer{},
			want:      []domainCount{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CountDomainsConcurrent(tt.customers)

			//special case for no data
			if len(got) == 0 && len(tt.want) == 0 {
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CountDomains() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadCustomersFromCSV(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []customer
		wantErr bool
	}{
		{
			name: "Valid CSV data",
			input: `first_name,last_name,email,gender,ip_address
First,Last,first.last@example.com,male,192.168.1.1
First,Last,first.last@example.com,female,192.168.1.2`,
			want: []customer{
				{FirstName: "First", LastName: "Last", Email: "first.last@example.com", Gender: male, IPAddress: net.ParseIP("192.168.1.1")},
				{FirstName: "First", LastName: "Last", Email: "first.last@example.com", Gender: female, IPAddress: net.ParseIP("192.168.1.2")},
			},
			wantErr: false,
		},
		{
			name: "Invalid CSV data - bad email",
			input: `first_name,last_name,email,gender,ip_address
			John,Doe,bademail,male,192.168.1.1`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := strings.NewReader(tt.input)
			got, err := ReadCustomersFromCSV(r)

			if err != nil && !tt.wantErr {
				t.Errorf("ReadCustomersFromCSV() unexpected error: %v", err)
				return
			}

			if err == nil && tt.wantErr {
				t.Errorf("ReadCustomersFromCSV() expected error, got none")
				return
			}

			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadCustomersFromCSV() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadAndCountDomainsFromCSV(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []domainCount
		wantErr bool
	}{
		{
			name: "Valid CSV data - single domain",
			input: `first_name,last_name,email,gender,ip_address
First,Last,first.last@example.com,male,192.168.1.1
First,Last,second.last@example.com,female,192.168.1.2`,
			want: []domainCount{
				{Domain: "example.com", Count: 2},
			},
			wantErr: false,
		},
		{
			name: "Valid CSV data - multiple domains",
			input: `first_name,last_name,email,gender,ip_address
First,Last,first.last@example1.com,male,192.168.1.1
First,Last,second.last@example2.com,female,192.168.1.2
First,Last,second.last@example1.com,female,192.168.1.2`,
			want: []domainCount{
				{Domain: "example1.com", Count: 2},
				{Domain: "example2.com", Count: 1},
			},
			wantErr: false,
		},
		{
			name: "Invalid CSV data - bad email",
			input: `first_name,last_name,email,gender,ip_address
First,Last,user1@@example.com,male,192.168.1.1`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := strings.NewReader(tt.input)
			got, err := ReadAndCountDomainsFromCSV(r)

			if err != nil && !tt.wantErr {
				t.Errorf("ReadAndCountDomainsFromCSV() unexpected error: %v", err)
				return
			}

			if err == nil && tt.wantErr {
				t.Errorf("ReadAndCountDomainsFromCSV() expected error, got none")
				return
			}

			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadAndCountDomainsFromCSV() got = %v, want %v", got, tt.want)
			}
		})
	}
}
