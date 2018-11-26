package manifest

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

//Entry represents an entries of a redshift manifest file
type Entry struct {
	Endpoint  string `json:"endpoint"`
	Command   string `json:"command"`
	Mandatory bool   `json:"mandatory"`
	PublicKey string `json:"publickey"`
	Username  string `json:"username"`
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

//CommandGenerator function used to populate the command attribute for an entry
type CommandGenerator func(file *s3.Object) string

//GenerateManifestFromS3WithBasicCredentials generate manifest using the aws basic credentials chain (env, shared credentials file, etc.)
func GenerateManifestFromS3WithBasicCredentials(region string, template Template, commandGenerator CommandGenerator, listObjectInput *s3.ListObjectsInput) (*Manifest, error) {

	awsConfig := &aws.Config{Region: aws.String(region)}
	s, err := session.NewSession(awsConfig)

	if err != nil {
		return nil, fmt.Errorf("Unable to stabilish a s3 session, %v", err)
	}

	return GenerateManifestFromS3(template, commandGenerator, s3.New(s), listObjectInput)
}

//GenerateManifestFromS3 generate manifest using the provided s3 session
func GenerateManifestFromS3(template Template, commandGenerator CommandGenerator, svc *s3.S3, listObjectInput *s3.ListObjectsInput) (*Manifest, error) {

	resp, err := svc.ListObjects(listObjectInput)

	if err != nil {
		return nil, fmt.Errorf("Unable to list items in bucket %q, %v", listObjectInput.Bucket, err)
	}

	var entries []Entry

	for _, item := range resp.Contents {
		entry := Entry{
			Endpoint:  *item.Key,
			Command:   commandGenerator(item),
			Mandatory: template.Mandatory,
			PublicKey: template.PublicKey,
			Username:  template.Username,
		}
		entries = append(entries, entry)

	}

	return &Manifest{entries}, nil

}
