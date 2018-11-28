package manifest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

//Entry represents an entries of a redshift manifest file
type Entry struct {
	URL       string `json:"url"`
	Command   string `json:"command,omitempty"`
	Mandatory bool   `json:"mandatory,omitempty"`
	PublicKey string `json:"publickey,omitempty"`
	Username  string `json:"username,omitempty"`
}

//Manifest represents a redshift manifest file
type Manifest struct {
	Entries []Entry `json:"entries"`
}

//Template template for generating the manifest entries
type Template struct {
	Mandatory bool
	PublicKey string
	Username  string
}

//Input struct with the necessary parameters to build the redshift manifest
type Input struct {
	Template            *Template
	CommandGenerator    CommandGenerator
	S3ObjectsInput      *s3.ListObjectsInput
	S3Session           *s3.S3
	ManifestDestination *s3.PutObjectInput
}

//CommandGenerator function used to populate the command attribute for an entry
type CommandGenerator func(file *s3.Object) string

//CopyExecutor function used to execute the generated copy command
type CopyExecutor func(manifestPath *string) error

//GenerateManifestFromS3 generate manifest using the provided s3 session
func GenerateManifestFromS3(input *Input) (*Manifest, error) {

	resp, err := input.S3Session.ListObjects(input.S3ObjectsInput)

	if err != nil {
		return nil, fmt.Errorf("Unable to list items in bucket %q, %v", input.S3ObjectsInput.Bucket, err)
	}

	var entries []Entry

	for _, item := range resp.Contents {

		//TODO is there a better way to check if it's a directory?
		entryName := *item.Key
		if entryName[len(entryName)-1:] == "/" {
			continue
		}

		entry := Entry{
			URL:       fmt.Sprintf("s3://%v/%v", *input.S3ObjectsInput.Bucket, *item.Key),
			Command:   input.CommandGenerator(item),
			Mandatory: input.Template.Mandatory,
			PublicKey: input.Template.PublicKey,
			Username:  input.Template.Username,
		}
		entries = append(entries, entry)

	}

	return &Manifest{entries}, nil

}

//GenerateAndWriteManifestFromS3 generate and write manifest using the provided s3 session
func GenerateAndWriteManifestFromS3(input *Input) (*Manifest, error) {

	manifest, err := GenerateManifestFromS3(input)

	if err != nil {
		return nil, err
	}

	manifestBytes, err := json.Marshal(manifest)

	if err != nil {
		return nil, fmt.Errorf("An unexpected error occurred while marshaling the manifest, %v", err)
	}

	input.ManifestDestination.Body = bytes.NewReader(manifestBytes)
	input.ManifestDestination.ContentLength = aws.Int64(int64(len(manifestBytes)))
	input.ManifestDestination.ContentType = aws.String(http.DetectContentType(manifestBytes))

	_, err = input.S3Session.PutObject(input.ManifestDestination)

	if err != nil {
		return nil, fmt.Errorf("An unexpected error occurred while writing the manifest into S3: %v", err)
	}

	return manifest, nil
}

//ExecuteCopyFromManifest triggers copyExecutor for the files in manifest and rename then with prefix "done"
func ExecuteCopyFromManifest(copyExecutor CopyExecutor, input *Input) error {

	manifest, err := GenerateAndWriteManifestFromS3(input)

	if err != nil {
		return err
	}

	if err = copyExecutor(input.ManifestDestination.Key); err != nil {
		return err
	}

	bucket := *input.S3ObjectsInput.Bucket

	for _, entry := range manifest.Entries {

		_, err = input.S3Session.CopyObject(&s3.CopyObjectInput{
			Bucket:     aws.String(bucket),
			CopySource: aws.String(fmt.Sprintf("/%s/%s", bucket, entry.URL)),
			Key:        aws.String(fmt.Sprintf("/done/%s", entry.URL)),
		})

		if err != nil {
			return fmt.Errorf("Unable to copy file %v. Error: %v", entry.URL, err)
		}

		_, err = input.S3Session.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(fmt.Sprintf("/%s", entry.URL)),
		})

		if err != nil {
			return fmt.Errorf("Unable to delete file %v. Error: %v", entry.URL, err)
		}

	}

	return nil
}
