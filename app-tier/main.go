package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"strings"
	"os/exec"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/s3"
)

// AWS SQS Queue URLs
var requestQueueURL = "https://sqs.us-east-1.amazonaws.com/12345678910/12345678910-req-queue"
var responseQueueURL = "https://sqs.us-east-1.amazonaws.com/12345678910/12345678910-resp-queue"

// S3 Bucket names
var inputBucket = "12345678910-in-bucket"
var outputBucket = "12345678910-out-bucket"

// AWS session
var sess = session.Must(session.NewSession(&aws.Config{
	Region: aws.String("us-east-1"),
}))

// SQS and S3 clients
var sqsClient = sqs.New(sess)
var s3Client = s3.New(sess)

// Poll SQS Request Queue for messages
func pollRequestQueue() (*sqs.Message, error) {
	result, err := sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(requestQueueURL),
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(10),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to poll request queue: %v", err)
	}

	if len(result.Messages) > 0 {
		return result.Messages[0], nil
	}

	return nil, nil
}

// Delete message from SQS Request Queue
func deleteMessageFromQueue(receiptHandle *string) error {
	_, err := sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(requestQueueURL),
		ReceiptHandle: receiptHandle,
	})
	if err != nil {
		return fmt.Errorf("failed to delete message from request queue: %v", err)
	}
	return nil
}

// Process image for inference
func processImage(messageBody string) (string, string, error) {
	// Parse the filename and base64 image data
	parts := strings.SplitN(messageBody, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid message format")
	}
	filename := parts[0]
	imageData, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", fmt.Errorf("failed to decode image data: %v", err)
	}

	// Save the image locally for inference
	imagePath := "/tmp/" + filename + ".jpg"
	err = os.WriteFile(imagePath, imageData, 0644)
	if err != nil {
		return "", "", fmt.Errorf("failed to write image file: %v", err)
	}

	// Perform inference (Assuming we have a Python script to perform inference)
	result, err := runInference(imagePath)
	if err != nil {
		return "", "", fmt.Errorf("failed to run inference: %v", err)
	}

	return filename, result, nil
}

// Run inference using a Python script
func runInference(imagePath string) (string, error) {
	cmd := exec.Command("python3", "model/inference.py", imagePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("inference error: %v", err)
	}
	return strings.TrimSpace(string(output)), nil
}

// Send classification result to Response Queue
func sendMessageToResponseQueue(filename, result string) error {
	messageBody := fmt.Sprintf("%s:%s", filename, result)
	_, err := sqsClient.SendMessage(&sqs.SendMessageInput{
		QueueUrl:    aws.String(responseQueueURL),
		MessageBody: aws.String(messageBody),
	})
	if err != nil {
		return fmt.Errorf("failed to send message to response queue: %v", err)
	}
	return nil
}

func main() {
	fmt.Println("App Tier started, polling for requests...")

	for {
		// Poll the request queue for messages
		message, err := pollRequestQueue()
		if err != nil {
			log.Printf("Error polling request queue: %v\n", err)
			continue
		}

		if message != nil {
			// Process the image and run inference
			filename, result, err := processImage(*message.Body)
			if err != nil {
				log.Printf("Error processing image: %v\n", err)
				continue
			}

			// Send the result back to the response queue
			err = sendMessageToResponseQueue(filename, result)
			if err != nil {
				log.Printf("Error sending message to response queue: %v\n", err)
				continue
			}

			// Delete the message from the request queue
			err = deleteMessageFromQueue(message.ReceiptHandle)
			if err != nil {
				log.Printf("Error deleting message from request queue: %v\n", err)
			}
		}
	}
}
