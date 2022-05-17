package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/joho/godotenv"
)

var (
	producer AWSKinesis
)

// AWSKinesis struct contain all field needed in kinesis stream
type AWSKinesis struct {
	stream          string
	region          string
	endpoint        string
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
}

type Entry struct {
	Type          string `json:"type"`
	Amount        int    `json:"amount"`
	Installment   int    `json:"installment"`
	EffectiveDate string `json:"effective_date"`
}

type Payment struct {
	MerchantId string  `json:"merchant_id"`
	PaymentId  string  `json:"payment_id"`
	Amount     int     `json:"amount"`
	Entry      []Entry `json:"entries"`
}

type Payments struct {
	Payments []Payment
}

// initiate configuration
func init() {
	e := godotenv.Load() //Load .env file
	if e != nil {
		fmt.Print(e)
	}
	producer = AWSKinesis{
		stream:          os.Getenv("KINESIS_STREAM_NAME"),
		region:          os.Getenv("KINESIS_REGION"),
		endpoint:        os.Getenv("AWS_ENDPOINT"),
		accessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		secretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		sessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
	}
}

func main() {
	// connect to aws
	s, err := session.NewSession(&aws.Config{
		Region:      aws.String(producer.region),
		Endpoint:    aws.String(producer.endpoint),
		Credentials: credentials.NewStaticCredentials(producer.accessKeyID, producer.secretAccessKey, producer.sessionToken),
	})
	client := kinesis.New(s)

	// verifica stream
	streamName := aws.String(producer.stream)
	_, err = client.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})
	//fmt.Printf("%v\n", *stream)
	if err != nil {
		log.Panic(err)
	}

	fmt.Println("Sending data to Kinesis")

	t1 := time.Now()
	for i := 0; i < 10; i++ {
		records := make([]*kinesis.PutRecordsRequestEntry, 500)
		for k := 0; k < 500; k++ {
			amount := rand.Intn(10000000)
			charge := Entry{Type: "charge", Amount: amount, Installment: 1, EffectiveDate: "2022-01-01"}
			chargeFee := Entry{Type: "charge_fee", Amount: 10, Installment: 1, EffectiveDate: "2022-01-01"}

			entries := []Entry{charge, chargeFee}
			payment := &Payment{MerchantId: strconv.Itoa(rand.Intn(10000)), PaymentId: strconv.Itoa(rand.Intn(10000)), Amount: amount, Entry: entries}

			b, err := json.Marshal(payment)
			if err != nil {
				fmt.Println(err)
				return
			}

			records[k] = &kinesis.PutRecordsRequestEntry{
				Data:         []byte(string(b) + "\n"),
				PartitionKey: aws.String("ledger"),
			}
		}

		//fmt.Println(xx)

		putOutput, err := client.PutRecords(&kinesis.PutRecordsInput{
			Records:    records,
			StreamName: streamName,
		})

		if err != nil {
			panic(err)
		}

		fmt.Printf("%v\n", *putOutput)
	}

	t2 := time.Now()
	diff := t2.Sub(t1)
	fmt.Println(diff)
}
