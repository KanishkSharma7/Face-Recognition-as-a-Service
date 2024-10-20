package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// AWS SQS Queue URLs
var requestQueueURL = "https://sqs.us-east-1.amazonaws.com/717279735481/1226213666-req-queue"
var responseQueueURL = "https://sqs.us-east-1.amazonaws.com/717279735481/1226213666-resp-queue"

// S3 Bucket names
var inputBucket = "1226213666-in-bucket"
var outputBucket = "1226213666-out-bucket"

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
		WaitTimeSeconds:     aws.Int64(20), // Increased wait time for efficient polling
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

// Upload image to S3 Input Bucket
func uploadImageToS3(inputBucket string, filename string, imageData []byte) error {
	_, err := s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(inputBucket),
		Key:    aws.String(filename),
		Body:   strings.NewReader(base64.StdEncoding.EncodeToString(imageData)),
	})
	if err != nil {
		return fmt.Errorf("failed to upload image to S3 bucket: %v", err)
	}
	return nil
}

// Upload classification result to S3 Output Bucket
func uploadResultToS3(outputBucket string, filename string, result string) error {
	_, err := s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(outputBucket),
		Key:    aws.String(filename),
		Body:   strings.NewReader(result),
	})
	if err != nil {
		return fmt.Errorf("failed to upload result to S3 bucket: %v", err)
	}
	return nil
}

// Process image for inference and upload results to S3
func processImage(messageBody string) (string, string, string, error) {
	// Parse the unique ID, filename, and base64 image data
	parts := strings.SplitN(messageBody, ":", 3)
	if len(parts) != 3 {
		return "", "", "", fmt.Errorf("invalid message format")
	}
	uniqueID := parts[0]
	filename := parts[1]
	imageData, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return "", "", "", fmt.Errorf("failed to decode image data: %v", err)
	}

	// Save the image locally for inference
	imagePath := "C:/Users/sharm/PersonalGithub/Face-Recognition-as-a-Service/tmp/" + filename + ".jpg"
	err = os.WriteFile(imagePath, imageData, 0644)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to write image file: %v", err)
	}
	log.Printf("Image written to: %s\n", imagePath)

	// Upload image to S3 Input Bucket
	err = uploadImageToS3(inputBucket, filename, imageData)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to upload image to S3: %v", err)
	}

	// Perform inference (Assuming we have a Python script to perform inference)
	result, err := runInference(imagePath)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to run inference: %v", err)
	}

	// Upload classification result to S3 Output Bucket
	err = uploadResultToS3(outputBucket, filename, result)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to upload result to S3: %v", err)
	}

	return uniqueID, filename, result, nil
}

// Run inference using a Python script
func runInference(imagePath string) (string, error) {
	cmd := exec.Command("python", "C:/Users/sharm/PersonalGithub/Face-Recognition-as-a-Service/app-tier/face_recognition.py", imagePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("inference error: %v, output: %s", err, string(output))
	}
	return strings.TrimSpace(string(output)), nil
}

// Send classification result to Response Queue
func sendMessageToResponseQueue(uniqueID, filename, result string) error {
	messageBody := fmt.Sprintf("%s:%s:%s", uniqueID, filename, result)
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
			uniqueID, filename, result, err := processImage(*message.Body)
			if err != nil {
				log.Printf("Error processing image: %v\n", err)
				continue
			}

			// Send the result back to the response queue
			err = sendMessageToResponseQueue(uniqueID, filename, result)
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
