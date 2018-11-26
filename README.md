# AWS Redshift Utils

This library is intended to provide basic interation between S3 and Redshift.

You could generate [Redshift Manifest file](https://docs.aws.amazon.com/redshift/latest/dg/load-from-host-steps-create-manifest.html) with the files you want for a later copy operation:

	var commandGenerator manifest.CommandGenerator = func(file *s3.Object) string {
		return fmt.Sprintf("cat %v", *file.Key)
	}

	listObjectINput := &s3.ListObjectsInput{
		Bucket: aws.String("my.s3.bucket.com"),
		Prefix: aws.String("path_prefix"),
	}

	template := manifest.Template{Mandatory: true}

	manifest := manifest.GenerateManifestFromS3WithBasicCredentials(endpoints.UsEast1RegionID, template, commandGenerator, listObjectINput)