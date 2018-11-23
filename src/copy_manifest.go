package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

//ManifestEntry represents an entries of a redshift manifest file
type ManifestEntry struct {
	Endpoint  string `json:"endpoint"`
	Command   string `json:"command"`
	Mandatory bool   `json:"mandatory"`
	PublicKey string `json:"publickey"`
	Username  string `json:"username"`
}

//Manifest represents a redshift manifest file
type Manifest struct {
	Entries []ManifestEntry `json:"entries"`
}

//ManifestTmpl TODO
type ManifestTmpl struct {
	Mandatory bool
	PublicKey string
	Username  string
}

//CommandGenerator TODO
type CommandGenerator func(file *s3.Object) string

func main() {

	var commandGenerator CommandGenerator = func(file *s3.Object) string {
		return fmt.Sprintf("cat %v", *file.Key)
	}

	awsConfig := &aws.Config{Region: aws.String(endpoints.UsEast1RegionID)}

	s, err := session.NewSession(awsConfig)
	if err != nil {
		fmt.Printf("Unable to stabilish a s3 session, %v", err)
	}

	svc := s3.New(s)

	listObjectINput := &s3.ListObjectsInput{
		Bucket: aws.String("notas-xml.triermais.com.br"),
		Prefix: aws.String("staging"),
	}

	template := ManifestTmpl{Mandatory: true}

	generateManifestFromS3(template, commandGenerator, svc, listObjectINput)

}

func generateManifestFromS3WithBasicCredentials(template ManifestTmpl, commandGenerator CommandGenerator, svc *s3.S3, listObjectInput *s3.ListObjectsInput) (*Manifest, error) {

	return generateManifestFromS3(template, commandGenerator, svc, listObjectInput)
}

func generateManifestFromS3(template ManifestTmpl, commandGenerator CommandGenerator, svc *s3.S3, listObjectInput *s3.ListObjectsInput) (*Manifest, error) {

	resp, err := svc.ListObjects(listObjectInput)

	if err != nil {
		return nil, fmt.Errorf("Unable to list items in bucket %q, %v", listObjectInput.Bucket, err)
	}

	var entries []ManifestEntry

	for _, item := range resp.Contents {
		entry := ManifestEntry{
			Endpoint:  *item.Key,
			Command:   commandGenerator(item),
			Mandatory: template.Mandatory,
			PublicKey: template.PublicKey,
			Username:  template.Username,
		}
		entries = append(entries, entry)
		fmt.Printf("File: %v \n", entry.Endpoint)
	}

	return &Manifest{entries}, nil

}
