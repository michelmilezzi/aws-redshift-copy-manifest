# AWS Redshift Utils

This library is intended to provide basic interation between S3 and Redshift.

You could generate [Redshift Manifest file](https://docs.aws.amazon.com/redshift/latest/dg/load-from-host-steps-create-manifest.html) for a later copy operation:

	var commandGenerator CommandGenerator = func(file *s3.Object) string {
		return fmt.Sprintf("cat %v", *file.Key)
	}

	listObjectINput := &s3.ListObjectsInput{
		Bucket: aws.String("my.s3.bucket.com"),
		Prefix: aws.String("path_prefix"),
	}

	template := ManifestTmpl{Mandatory: true}

	manifest := GenerateManifestFromS3WithBasicCredentials(endpoints.UsEast1RegionID, template, commandGenerator, listObjectINput)