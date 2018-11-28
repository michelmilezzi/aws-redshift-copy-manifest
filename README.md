# AWS Redshift Utils

This library is intended to provide a basic interaction between S3 and Redshift.

You could generate a [Redshift Manifest file](https://docs.aws.amazon.com/redshift/latest/dg/load-from-host-steps-create-manifest.html) with the files you want for a later copy operation:

```go
	//building a basic aws s3 session
	awsConfig := &aws.Config{Region: aws.String(endpoints.UsEast1RegionID)}

	s, err := session.NewSession(awsConfig)
	
	if err != nil {
		fmt.Printf("Unable to stabilish a s3 session, %v", err)
		os.Exit(1)
	}

	svc := s3.New(s)

	//objects that will be included on manifest file
	listObjectINput := &s3.ListObjectsInput{
		Bucket: aws.String("my-bucket"),
		Prefix: aws.String("path-prefix-if-any"),
	}

	//template for the manifest struct
	template := &manifest.Template{Mandatory: true}

	//destination for manifest file
	manifestDestination := &s3.PutObjectInput{
		Bucket:               aws.String("my-bucket"),
		Key:                  aws.String("my.manifest"),
		ACL:                  aws.String("private"),
		ContentDisposition:   aws.String("attachment"),
		ServerSideEncryption: aws.String("AES256"),
	}

	//function that builds the manifest command string for each object
	var commandGenerator manifest.CommandGenerator = func(file *s3.Object) string {
		return fmt.Sprintf("cat %v", *file.Key)
	}

	//putting all together
	input := manifest.Input{
		CommandGenerator:    commandGenerator,
		S3ObjectsInput:      listObjectINput,
		S3Session:           svc,
		Template:            template,
		ManifestDestination: manifestDestination,
	}

	//generating...
	_, err = manifest.GenerateAndWriteManifestFromS3(&input)

	if err != nil {
		fmt.Printf("Unexpected error: %v", err)
		os.Exit(1)
	}	
```
