package as3

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	appconf "db-snapshot/internal/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type AS3 struct {
	log     *slog.Logger
	appConf appconf.AppConfig
	config  *aws.Config
}

func New(log *slog.Logger, appConf appconf.AppConfig) *AS3 {
	return &AS3{
		log:     log,
		appConf: appConf,
	}
}

func (as3 *AS3) Init(ctx context.Context) error {
	conf, err := config.LoadDefaultConfig(ctx, config.WithRegion(as3.appConf.AWSRegion))
	as3.config = &conf
	as3.log.Info("Amazon S3 config initialized", "region", as3.appConf.AWSRegion)
	if err != nil {
		return err
	}

	client := s3.NewFromConfig(*as3.config)

	_, err = client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(as3.appConf.AmazonS3Bucket),
		ACL:    types.BucketCannedACLPrivate,
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(as3.appConf.AWSRegion),
		},
	})
	if err != nil {
		existsErr := &types.BucketAlreadyOwnedByYou{}
		if !errors.As(err, &existsErr) {
			return err
		}
	}
	as3.log.Info("Created bucket if not exists", "bucket", as3.appConf.AmazonS3Bucket)

	return nil
}

func (as3 *AS3) Upload(filePath string, objectKey string) error {
	if as3.config == nil {
		return fmt.Errorf("AS3 has no config initialized")
	}

	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	s3Client := s3.NewFromConfig(*as3.config)
	_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(as3.appConf.AmazonS3Bucket),
		Key:    aws.String(objectKey),
		Body:   f,
	})
	if err != nil {
		return err
	}

	return nil

}

func (as3 *AS3) CleanUp(till time.Time) error {
	client := s3.NewFromConfig(*as3.config)

	contents, err := fetchObjectsToDelete(client, as3.appConf.AmazonS3Bucket)
	if err != nil {
		return err
	}

	toDeleteObjs := getToDeleteObjects(contents, till)
	if len(toDeleteObjs) == 0 {
		as3.log.Info("No S3 objects to delete")
		return nil
	}

	_, err = client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
		Bucket: aws.String(as3.appConf.AmazonS3Bucket),
		Delete: &types.Delete{
			Objects: toDeleteObjs,
			Quiet:   aws.Bool(true),
		},
	})

	return err
}

func fetchObjectsToDelete(client *s3.Client, bucket string) ([]types.Object, error) {
	listOut, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return []types.Object{}, err
	}

	nextContToken := listOut.NextContinuationToken
	contents := listOut.Contents
	for nextContToken != nil {
		listOut, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: nextContToken,
		})
		if err != nil {
			return []types.Object{}, err
		}

		contents = append(contents, listOut.Contents...)
		nextContToken = listOut.NextContinuationToken
	}

	return contents, nil
}

func getToDeleteObjects(contents []types.Object, till time.Time) []types.ObjectIdentifier {
	var toDeleteObjs = make([]types.ObjectIdentifier, 0, len(contents))
	for _, obj := range contents {
		if obj.LastModified.Before(till) {
			toDeleteObjs = append(toDeleteObjs, types.ObjectIdentifier{Key: obj.Key})
		}
	}

	return toDeleteObjs

}

func GetObjectKeyFromPath(filePath string) string {
	parts := strings.Split(filePath, "/")
	return parts[len(parts)-1]
}
