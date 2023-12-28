package cmd

import (
	"fmt"
	"math"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

func s3headObject(svc *s3.S3, bucketName string, objectKey string) (*s3.HeadObjectOutput, bool, error) {
	// Define the parameters for the HeadObject API call
	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}

	// Make the API call
	resp, err := svc.HeadObject(params)
	if err != nil {
		//fmt.Println("Error getting object metadata:", err)
		return nil, false, err
	}

	return resp, true, nil
}

// Lists objects for a prefix
func s3listObjects(svc *s3.S3, bucketName string, prefix string, startKey string, startVersion string, maxKeys int64) (*s3.ListObjectsOutput, error) {
	//Define the parameters for the listObject API call
	params := &s3.ListObjectsInput{
		Bucket:  aws.String(bucketName),
		MaxKeys: aws.Int64(maxKeys),
		//FIX REMOVE:   Delimiter: aws.String("/"),
	}

	if prefix != "" {
		params.Prefix = aws.String(prefix)
	}

	if startKey != "" {
		params.Marker = aws.String(startKey)
	}

	//Make the API call
	resp, err := svc.ListObjects(params)
	if err != nil {
		return nil, err
	}
	//return resp.Contents, nil --> returns a []*s3.Object
	return resp, nil
}

func s3ListObjectsWithBackOff(svc *s3.S3, bucketName string, prefix string, startKey string, startVersion string, maxKeys int64) (*s3.ListObjectsV2Output, error) {

	maxRetries := 10

	for {
		//Define the parameters for the listObject API call
		params := &s3.ListObjectsV2Input{
			Bucket:  aws.String(bucketName),
			MaxKeys: aws.Int64(maxKeys),
			//FIX REMOVE:   Delimiter: aws.String("/"),
		}

		if prefix != "" {
			params.Prefix = aws.String(prefix)
		}

		for i := 0; ; i++ {
			//Make the API call
			resp, err := svc.ListObjectsV2(params)
			if err != nil {
				return resp, nil
			}
			if err == nil {
				return resp, nil
			} else {
				if awsErr, ok := err.(awserr.Error); ok {
					switch awsErr.Code() {
					case s3.ErrCodeNoSuchBucket:
						return nil, fmt.Errorf("bucket %s does not exist: %w", bucketName, err)
					default:
						if i >= maxRetries {
							return nil, fmt.Errorf("too many failed attempts to list objects: %w", err)
						}

						wait := time.Duration(math.Exp2(float64(i))) * time.Second
						TracePrintln("got error", err, "retrying after", wait)
						time.Sleep(wait)
					}
				} else {
					return nil, fmt.Errorf("unknown error occurred: %w", err)
				}
			}
		}
	}
}

func s3ListAllObjectsWithBackoff(svc *s3.S3, bucketName string, prefix string, startKey string, startVersion string, maxKeys int64, chs3Object chan<- *s3.Object) (int, error) {

	var continuationToken *string
	var thiscount int
	maxRetries := 10

	for {
		params := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucketName),
			ContinuationToken: continuationToken,
			MaxKeys:           aws.Int64(maxKeys),
		}
		if prefix != "" {
			params.Prefix = aws.String(prefix)
		}

		for i := 0; ; i++ {
			result, err := svc.ListObjectsV2(params)
			if err == nil {
				for _, object := range result.Contents {

					chs3Object <- object
					thiscount++
				}
				if result.IsTruncated != nil && *result.IsTruncated {
					continuationToken = result.NextContinuationToken
				} else {
					continuationToken = nil
				}
				break
			} else {
				if awsErr, ok := err.(awserr.Error); ok {
					switch awsErr.Code() {
					case s3.ErrCodeNoSuchBucket:
						return 0, fmt.Errorf("bucket %s does not exist: %w", bucketName, err)
						//return nil, fmt.Errorf("bucket %s does not exist: %w", bucketName, err)

					default:
						if i >= maxRetries {
							return 0, fmt.Errorf("too many failed attempts to list objects: %w", err)
							//return nil, fmt.Errorf("too many failed attempts to list objects: %w", err)

						}

						wait := time.Duration(math.Exp2(float64(i))) * time.Second
						TracePrintln("got error", err, "retrying after", wait)
						time.Sleep(wait)
					}
				} else {
					//return nil, fmt.Errorf("unknown error occurred: %w", err)
					return 0, fmt.Errorf("unknown error occurred: %w", err)

				}
			}
		}

		if continuationToken == nil {
			break
		}
	}

	return thiscount, nil
}

func s3listObjectsFilter(svc *s3.S3, bucketName string, prefix string, startKey string, startVersion string, maxKeys int64) ([]*s3.Object, error) {
	//Define the parameters for the listObject API call
	params := &s3.ListObjectsInput{
		Bucket:  aws.String(bucketName),
		MaxKeys: aws.Int64(maxKeys),
		//FIX REMOVE: Delimiter: aws.String("/"),
	}

	if prefix != "" {
		params.Prefix = aws.String(prefix)
	}
	if startKey != "" {
		params.Marker = aws.String(startKey)
	}

	//Make the API call
	resp, err := svc.ListObjects(params)
	if err != nil {
		return nil, err
	}

	var filteredObjects []*s3.Object
	for _, v := range resp.Contents {
		if *v.Key == prefix {
			filteredObjects = append(filteredObjects, v)
		}
	}

	return filteredObjects, nil
}

// Lists object versions for a prefix, starting from the given key and version ID
func s3listObjectVersions(svc *s3.S3, bucketName string, prefix string, startKey string, startVersion string, maxKeys int64) (*s3.ListObjectVersionsOutput, error) {
	//Define the parameters for the listObjectVersions API call
	params := &s3.ListObjectVersionsInput{
		Bucket:  aws.String(bucketName),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(maxKeys),
		//FIX REMOVE: Delimiter: aws.String("/"),
	}

	if startKey != "" {
		params.KeyMarker = aws.String(startKey)
	}

	if startVersion != "" {
		params.VersionIdMarker = aws.String(startVersion)
	}
	//Make the API call
	resp, err := svc.ListObjectVersions(params)
	if err != nil {
		return nil, err
	}

	//return resp.Versions, nil -> returns a []*s3.ObjectVersion
	return resp, nil
}

// Lists object versions for a key and only that key
func s3listObjectVersionsFilter(svc *s3.S3, bucketName string, key string, startKey string, startVersion string, maxKeys int64) ([]*s3.ObjectVersion, error) {
	//Define the parameters for the listObjectVersions API call
	params := &s3.ListObjectVersionsInput{
		Bucket:  aws.String(bucketName),
		Prefix:  aws.String(key),
		MaxKeys: aws.Int64(maxKeys),
		//FIX REMOVE: Delimiter: aws.String("/"),
	}

	if startKey != "" {
		params.KeyMarker = aws.String(startKey)
	}

	if startVersion != "" {
		params.VersionIdMarker = aws.String(startVersion)
	}
	//Make the API call
	resp, err := svc.ListObjectVersions(params)
	if err != nil {
		return nil, err
	}

	var filteredVersions []*s3.ObjectVersion
	for _, v := range resp.Versions {
		if *v.Key == key {
			filteredVersions = append(filteredVersions, v)
		}
	}

	return filteredVersions, nil

}

// Gets the location of a bucket
func getBucketLocation(svc *s3.S3, fBucketName string) (string, error) {
	resp, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{Bucket: aws.String(fBucketName)})
	if err != nil {
		return "", err
	}

	// If the location is not set or is us-east-1, return an empty string (us-east-1 is the default region)
	if resp.LocationConstraint == nil || *resp.LocationConstraint == "us-east-1" {
		return "", nil
	}

	return *resp.LocationConstraint, nil
}
