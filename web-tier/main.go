package main

import (
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// AWS SQS Queue URLs (replace these with your actual queue URLs)
var requestQueueURL = "https://sqs.us-east-1.amazonaws.com/12345678910/12345678910-req-queue"
var responseQueueURL = "https://sqs.us-east-1.amazonaws.com/12345678910/12345678910-resp-queue"

// Create a session for AWS
var sess = session.Must(session.NewSession(&aws.Config{
	Region: aws.String("us-east-1"),
}))

// SQS Client
var sqsClient = sqs.New(sess)

// Send image data to the App Tier via SQS Request Queue
func sendMessageToRequestQueue(imageData []byte, filename string) error {
	// Encode the image data to base64 for safe transmission
	messageBody := base64.StdEncoding.EncodeToString(imageData)

	// Send the message to the request queue
	_, err := sqsClient.SendMessage(&sqs.SendMessageInput{
		QueueUrl:    aws.String(requestQueueURL),
		MessageBody: aws.String(fmt.Sprintf("%s:%s", filename, messageBody)),
	})
	if err != nil {
		return fmt.Errorf("failed to send message to request queue: %v", err)
	}
	return nil
}

// Poll SQS Response Queue for the classification result
func receiveMessageFromResponseQueue() (string, error) {
	for {
		// Poll for the message from the response queue
		result, err := sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(responseQueueURL),
			MaxNumberOfMessages: aws.Int64(1),
			WaitTimeSeconds:     aws.Int64(10),
		})
		if err != nil {
			return "", fmt.Errorf("failed to receive message from response queue: %v", err)
		}

		if len(result.Messages) > 0 {
			// Return the message body (classification result) and delete it from the queue
			classificationResult := *result.Messages[0].Body

			// Delete the message from the response queue
			_, err = sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      aws.String(responseQueueURL),
				ReceiptHandle: result.Messages[0].ReceiptHandle,
			})
			if err != nil {
				return "", fmt.Errorf("failed to delete message from response queue: %v", err)
			}
			return classificationResult, nil
		}
		time.Sleep(1 * time.Second)
	}
}

// HTTP handler for prediction requests
func predictionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// Parse the uploaded file
	file, header, err := r.FormFile("inputFile")
	if err != nil {
		http.Error(w, "Failed to read input file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Read the file content
	fileBytes, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, "Failed to read file content", http.StatusInternalServerError)
		return
	}

	// Extract the filename without extension
	filename := strings.TrimSuffix(header.Filename, filepath.Ext(header.Filename))

	// Send image data to SQS Request Queue
	err = sendMessageToRequestQueue(fileBytes, filename)
	if err != nil {
		http.Error(w, "Failed to send request to App Tier", http.StatusInternalServerError)
		return
	}

	// Poll the SQS Response Queue for classification result
	result, err := receiveMessageFromResponseQueue()
	if err != nil {
		http.Error(w, "Failed to receive result from App Tier", http.StatusInternalServerError)
		return
	}

	// Respond to the client with the classification result
	fmt.Fprintf(w, "%s", result)
}

func main() {
	// Set up HTTP handler for image uploads and predictions
	http.HandleFunc("/", predictionHandler)

	fmt.Println("Web Tier server started at :8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
