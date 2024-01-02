/*
Copyright © 2023 Jean-Baptiste Thomas <jboothomas@gmail.com>
This file is part of CLI application pS3.
*/
package cmd

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/cobra"
)

// listObjectsV2Cmd represents the listObjectsV2 command
var listObjectsV2Cmd = &cobra.Command{
	Use:   "list-objects-v2",
	Short: "Returns some or all of the objects in a bucket.",
	Long:  `To use this action you must have permissions to perform the s3:ListBucket action.`,
	Run: func(cmd *cobra.Command, args []string) {
		fBucketName, _ := cmd.Flags().GetString("bucket")
		fPrefixCount, _ := cmd.Flags().GetInt("prefix-count")
		listObjectsV2(fBucketName, fPrefixCount, fEndpointUrl, fProfile, fRegion, fNoVerifySSL, fOutput)
	},
}

func init() {
	rootCmd.AddCommand(listObjectsV2Cmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	//listObjectsV2Cmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// listObjectsV2Cmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	listObjectsV2Cmd.Flags().String("bucket", "", "Bucket name to list (required)")
	listObjectsV2Cmd.MarkFlagRequired("bucket")
	listObjectsV2Cmd.Flags().Int("prefix-count", 500, "Prefix count for distribution calculation. The number is the point where prefixes above 1000 objects are passed for processing.")

}

func listObjectsV2(fBucketName string, fPrefixCount int, fEndpointUrl string, fProfile string, fRegion string, fNoVerifySSL bool, fOutput string) {

	TracePrintln("trace: list-objects-v2 bucket: ", fBucketName, "endpoint: ", fEndpointUrl, "profile: ", fProfile, "region: ", fRegion, "no_ssl: ", fNoVerifySSL, "output: ", fOutput, "prefix-count: ", fPrefixCount)

	//build s3 api session
	httpClient, err := NewHTTPClientWithSettings(HTTPClientSettings{
		Connect:               5 * time.Second,
		ExpectContinue:        1 * time.Second,
		IdleConn:              30 * time.Second, //90
		ConnKeepAlive:         10 * time.Second, //30
		MaxAllIdleConns:       100,
		MaxHostIdleConns:      100, // This setting is important for concurrent HEAD requests
		ResponseHeader:        5 * time.Second,
		TLSHandshake:          5 * time.Second,
		TLSInsecureSkipVerify: fNoVerifySSL,
	})
	if err != nil {
		log.Fatalf("Error creating custom HTTP client: %v\n", err)
		//os.Exit(1) called implicitly by log.Fatalf
	}

	s3Config := &aws.Config{
		DisableSSL:       aws.Bool(fNoVerifySSL),
		S3ForcePathStyle: aws.Bool(true),
		HTTPClient:       httpClient,
		//Region:           aws.String("us-east-1"),
		//Credentials:      credentials.NewSharedCredentials("", fProfile),
	}

	if fEndpointUrl != "" {
		s3Config.Endpoint = &fEndpointUrl
	}

	//if fProfile != "" {
	//	s3Config.Credentials = credentials.NewSharedCredentials("", fProfile)
	//}

	if fRegion != "" {
		s3Config.Region = &fRegion
	}

	sess, err := session.NewSessionWithOptions(session.Options{
		// Specify profile to load for the session's config
		Profile: fProfile,

		// Provide SDK Config options, such as Region.
		Config: *s3Config,

		// Force enable Shared Config support
		SharedConfigState: session.SharedConfigEnable,
	})

	if err != nil {
		log.Fatalln("error: S3 session creation failed")
	}

	svc := s3.New(sess)

	location, err := getBucketLocation(svc, fBucketName)
	if err != nil {
		DebugPrintln("Error getting location for bucket or endpoint does not have a 'region'", fBucketName, ":", err)
	}

	if location != fRegion {
		TracePrintln("trace: bucket region is: ", location)
		s3Config.Region = aws.String(location)
	} else {
		s3Config.Region = aws.String("us-west-1")
	}

	sess, err = session.NewSessionWithOptions(session.Options{
		// Specify profile to load for the session's config
		Profile: fProfile,

		// Provide SDK Config options, such as Region.
		Config: *s3Config,

		// Force enable Shared Config support
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		log.Fatalln("error: S3 session creation failed")
	}

	svc = s3.New(sess)

	//variables / structures for object processing
	chs3Object := make(chan *s3.Object)
	done := make(chan bool)
	var prefixes []string
	//sync waitgroup
	var wg sync.WaitGroup

	go func() {
		findPrefixes(svc, fBucketName, "", fPrefixCount, chs3Object, &wg, &prefixes, fDebug)
		wg.Wait()
		listObjectsInParallel(svc, fBucketName, prefixes, chs3Object, &wg, fDebug)
		wg.Wait()
		close(chs3Object)
		done <- true
	}()
	readObjectsV2(fOutput, chs3Object, done)

}


func readObjectsV2(fOutput string, ch3Object <-chan *s3.Object, done <-chan bool) {
        var wg sync.WaitGroup
	numWorkers := maxSemaphore
	var objCount int32 = 0 //int32 for atomic operations

	//worker function
	worker := func() {
		defer wg.Done()
		for item := range ch3Object {
			if fDebug || fTrace {
				atomic.AddInt32(&objCount, 1)
				} else {
				fmt.Printf("Object: %v \t %d \t %s\n", *item.LastModified, *item.Size, *item.Key)
				}
			}
		}

	//start workers
	wg.Add(numWorkers)
	for i := 0; i< numWorkers; i++ {
		go worker()
		}

	//wait for done signal
	<-done

	//wait for workes to complete
	wg.Wait()

	//debug print the objectCount
	if fDebug || fTrace {
		fmt.Println("debug: item count=". atomic.LoadInt32(&objCount))
		}
}

func findPrefixes(svc *s3.S3, fBucketName, prefix string, target int, chs3Object chan<- *s3.Object, wg *sync.WaitGroup, prefixes *[]string, fDebug bool) {

	var mu sync.Mutex
	var processedCount int

	var discoverPrefixes func(string)
	discoverPrefixes = func(currentPrefix string) {
		defer wg.Done()

		mu.Lock()
		thisProcessedCount := processedCount
		thislenPrefixes := len(*prefixes)
		mu.Unlock()
		TracePrintln("trace: processed small prefixes processed: ", thisProcessedCount, " 'large' prefixes discoverd: ", thislenPrefixes)

		if thisProcessedCount >= target {
			TracePrintln("trace: prefix overload", currentPrefix)

			mu.Lock()
			*prefixes = append(*prefixes, currentPrefix)
			mu.Unlock()
			return
		}
		for _, c := range characters {
			nextPrefix := currentPrefix + c

			resp, err := s3ListObjectsWithBackOff(svc, fBucketName, nextPrefix, "", "", maxKeys)
			if err != nil {
				log.Fatalln("Error listing objects:", err)
				//os.Exit(1) called implicitly by log.Fatal
			}

			objectCount := len(resp.Contents)

			if objectCount > 999 {

				if nextPrefix == *resp.Contents[0].Key {
					//unique key with prefix found
					TracePrintln("trace: 'single' prefix=key: ", prefix)

					chs3Object <- resp.Contents[0]

				}

				mu.Lock()
				processedCount++
				thisProcessedCount := processedCount
				mu.Unlock()
				TracePrintln("trace: current discovered 'large' prefix count at ", thisProcessedCount)

				wg.Add(1)
				go discoverPrefixes(nextPrefix)

			} else if objectCount > 0 {

				mu.Lock()
				processedCount++
				thisProcessedCount := processedCount
				mu.Unlock()
				TracePrintln("trace: current processed 'small' prefix count at ", thisProcessedCount)
				TracePrintln("trace: 'small' prefix: ", nextPrefix, "item count: ", len(resp.Contents))

				for i := range resp.Contents {
					chs3Object <- resp.Contents[i]
				}
			}
		}
	}

	wg.Add(1)
	discoverPrefixes(prefix)
	wg.Wait()

	//Loop to rebuild prefixes if too low when compared to target count
	//Loop runs a total 10 times and then we stop so as to not iterate down to zero and make no progress
	prefix_iterate := 0
	for len(*prefixes) > 0 && prefix_iterate < 10 {
		DebugPrintln("debug: large prefixes", len(*prefixes), "discovered but target is", target, "re-iterate", prefix_iterate, "/5")

		if len(*prefixes) < target {

			prefix_iterate++
			DebugPrintln("debug: prefix count too low ", len(*prefixes))

			mu.Lock()
			processedCount = processedCount * 3 / 4
			mu.Unlock()

			oldPrefixes := *prefixes
			*prefixes = nil

			for _, p := range oldPrefixes {
				wg.Add(1)
				discoverPrefixes(p)
			}
		} else {
			break
		}
	}
}

func listObjectsInParallel(svc *s3.S3, fBucketName string, prefixes []string, chs3Object chan<- *s3.Object, wg *sync.WaitGroup, fDebug bool) {
	// semaphore defines slots available
	// using struct{} as lowest memory consumption for just a slot tracker
	semaphore := make(chan struct{}, maxSemaphore)
	// Additional channel to signal that all goroutines have been scheduled
	scheduled := make(chan struct{}, len(prefixes))

	DebugPrintln("debug: Large Prefixes to process", len(prefixes))

	for _, prefix := range prefixes {
		pcount := 0
		wg.Add(1)
		go func(prefix string) {
			semaphore <- struct{}{}
			scheduled <- struct{}{}

			defer wg.Done()
			defer func() {
				<-semaphore // release a slot
			}()

			tcount, err := s3ListAllObjectsWithBackoff(svc, fBucketName, prefix, "", "", 1000, chs3Object)
			pcount = tcount

			if err != nil {
				log.Fatalln("Error listing objects for prefix:", prefix, err)
				//os.Exit(1) called implicitly by log.Fatal
			}
			TracePrintln("trace: 'large' prefix", prefix, "item count: ", pcount)

		}(prefix)
	}
	// Wait for all goroutines to be scheduled before waiting for their completion
	for i := 0; i < len(prefixes); i++ {
		<-scheduled
	}
}
