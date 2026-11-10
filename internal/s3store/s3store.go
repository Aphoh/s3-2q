// Package s3store implements evictor.Store with the AWS SDK.
package s3store

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Store struct{ c *s3.Client }

func New(endpoint, region, accessKey, secretKey string) *Store {
	c := s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       region,
		Credentials:  credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		UsePathStyle: true,
		HTTPClient:   &http.Client{Timeout: 60 * time.Second},
	})
	return &Store{c: c}
}

func (s *Store) ListBuckets(ctx context.Context) ([]string, error) {
	var out []string
	p := s3.NewListBucketsPaginator(s.c, &s3.ListBucketsInput{})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, b := range page.Buckets {
			out = append(out, aws.ToString(b.Name))
		}
	}
	return out, nil
}

func (s *Store) ListObjects(ctx context.Context, bucket string, fn func(string, int64, time.Time)) error {
	p := s3.NewListObjectsV2Paginator(s.c, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return err
		}
		for _, o := range page.Contents {
			fn(aws.ToString(o.Key), aws.ToInt64(o.Size), aws.ToTime(o.LastModified))
		}
	}
	return nil
}

func (s *Store) Head(ctx context.Context, bucket, key string) (int64, bool, error) {
	out, err := s.c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		var re *awshttp.ResponseError
		if errors.As(err, &re) && re.HTTPStatusCode() == http.StatusNotFound {
			return 0, false, nil
		}
		return 0, false, err
	}
	return aws.ToInt64(out.ContentLength), true, nil
}

func (s *Store) Delete(ctx context.Context, bucket, key string) error {
	_, err := s.c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	return err
}
