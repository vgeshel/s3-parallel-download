package main

import (
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
	"fmt"
	"flag"
	"time"
	"os"
	"net/http"
	"crypto/tls"
	"strings"
	"io"
)

// scope github.com/vgeshel/s3-parallel-download github.com/mitchellh/goamz/aws github.com/mitchellh/goamz/s3 fmt flag time os net/http crypto/tls


func main() {
	// dir := flag.String("dir", ".", "output directory")
	region := flag.String("region", aws.USEast.Name, "region")
	verbose := flag.Bool("v", false, "verbose")
	insecure := flag.Bool("insecure", false, "turn on InsecureSkipVerify: http://golang.org/pkg/crypto/tls/#Config")
	//max_attempts := flag.Int64("attempts", 5, "the maximum number of attempts to read the object")
	//delay := flag.Int64("delay", 5, "seconds to sleep between attempts")

	flag.Parse()

	auth, err := aws.GetAuth("", "")

	if err != nil {
		fmt.Fprintf(os.Stderr, "auth error: %#v\n", err)
		os.Exit(10)
	}

	if *verbose {
		fmt.Fprintf(os.Stderr, "auth: %#v\n", auth)
	}

	reg, found := aws.Regions[*region]

	if ! found {
		fmt.Fprintf(os.Stderr, "invalid region %s\n", *region)
		os.Exit(11)
	}

	s3c := s3.New(auth, reg)

	done := make(chan string)
	before := time.Now()
	cnt := 0

	for i := 0; i < flag.NArg(); i ++ {
		path := flag.Arg(i)
		split := strings.SplitN(path, "/", 2)
		bucket := split[0]
		key := split[1]
		cnt ++

		go doGet(s3c, bucket, key, done, *insecure, before, *verbose)
	}

	if cnt > 0 {
		for {
			donePath := <-done;

			if *verbose {
				fmt.Printf("done %s in %v\n", donePath, time.Now().Sub(before))
			}

			cnt --

			if cnt <= 0 {
				break
			}
		}
	}

	fmt.Printf("done ALL in %v\n", time.Now().Sub(before))

}

func doGet(s3c *s3.S3, bucket string, key string, done chan string, insecure bool, before time.Time, verbose bool) {
	path := bucket + "/" + key

	if verbose {
		fmt.Printf("starting %s in %v\n", path, time.Now().Sub(before))
	}

	if insecure {
		tr := &http.Transport{
			TLSClientConfig:    &tls.Config{InsecureSkipVerify: true},
		}
		http.DefaultClient.Transport = tr
	}

	buck := s3c.Bucket(bucket)
	reader, err := buck.GetReader(key)

	if err != nil {
		done <- "ERROR " + fmt.Sprintf("%v", err) + ": " + bucket + "/" + key
	} else {
		writer, err := os.Create("/dev/null")

		if err != nil {
			panic(err)
		}

		if verbose {
			fmt.Printf("starting copy %s in %v\n", path, time.Now().Sub(before))
		}

		io.Copy(writer, reader)

		done <- bucket + "/" + key
	}

}
