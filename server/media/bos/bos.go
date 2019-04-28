// Package s3 implements media interface by storing media objects in Amazon S3 bucket.
package bos

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/baidubce/bce-sdk-go/services/bos"
	"github.com/baidubce/bce-sdk-go/services/bos/api"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"os"
	"sync/atomic"

	"github.com/tinode/chat/server/media"
	"github.com/tinode/chat/server/store"
	"github.com/tinode/chat/server/store/types"
)

const (
	defaultServeURL = "/v0/file/s/"
	handlerName     = "bos"
)

type bosConfig struct {
	AccessKeyId     string   `json:"access_key_id"`
	SecretAccessKey string   `json:"secret_access_key"`
	Region          string   `json:"region"`
	BucketName      string   `json:"bucket"`
	CorsOrigins     []string `json:"cors_origins"`
	ServeURL        string   `json:"serve_url"`
}

type bosHandler struct {
	svc  *bos.Client
	conf bosConfig
}

// readerCounter is a byte counter for bytes read through the io.Reader
type readerCounter struct {
	io.Reader
	count  int64
	reader io.Reader
}

// Read reads the bytes and records the number of read bytes.
func (rc *readerCounter) Read(buf []byte) (int, error) {
	n, err := rc.reader.Read(buf)
	atomic.AddInt64(&rc.count, int64(n))
	return n, err
}

// Init initializes the media handler.
func (bh *bosHandler) Init(jsconf string) error {
	var err error
	if err = json.Unmarshal([]byte(jsconf), &bh.conf); err != nil {
		return errors.New("failed to parse config: " + err.Error())
	}

	if bh.conf.AccessKeyId == "" {
		return errors.New("missing Access Key ID")
	}
	if bh.conf.SecretAccessKey == "" {
		return errors.New("missing Secret Access Key")
	}
	if bh.conf.Region == "" {
		return errors.New("missing Region")
	}
	if bh.conf.BucketName == "" {
		return errors.New("missing Bucket")
	}

	if bh.conf.ServeURL == "" {
		bh.conf.ServeURL = defaultServeURL
	}

	ak, sk := bh.conf.AccessKeyId, bh.conf.SecretAccessKey
	endpoint := bh.conf.Region
	// Create bos service client
	bh.svc, err = bos.NewClient(ak, sk, endpoint)
	if err != nil {
		return errors.New("bos new client:" + err.Error())
	}
	//bh.svc.Config.ConnectionTimeoutInMillis

	// Check if the bucket exists, create one if not.
	exist, _ := bh.svc.DoesBucketExist(bh.conf.BucketName)
	if !exist {
		_, err = bh.svc.PutBucket(bh.conf.BucketName)
		if err != nil {
			return errors.New("create bucket failed:" + err.Error())
		}
	}

	// The following serves two purposes:
	// 1. Setup CORS policy to be able to serve media directly from S3
	// 2. Verify that the bucket is accessible to the current user.
	origins := bh.conf.CorsOrigins
	if len(origins) == 0 {
		origins = append(origins, "*")
	}
	obj := &api.PutBucketCorsArgs{
		CorsConfiguration: []api.BucketCORSType{
			api.BucketCORSType{
				AllowedOrigins: bh.conf.CorsOrigins,
				AllowedMethods: []string{"GET"},
				MaxAgeSeconds:  1200,
			},
		},
	}
	err = bh.svc.PutBucketCorsFromStruct(bh.conf.BucketName, obj)

	return err
}

// Redirect is used when one wants to serve files from a different external server.
func (bh *bosHandler) Redirect(upload bool, url string) (string, error) {
	//return "", types.ErrUnsupported
	return "", nil
}

// Upload processes request for a file upload. The file is given as io.Reader.
func (bh *bosHandler) Upload(fdef *types.FileDef, file io.ReadSeeker) (string, error) {
	var err error

	fname := fdef.Id
	ext, _ := mime.ExtensionsByType(fdef.MimeType)
	if len(ext) > 0 {
		fname += ext[0]
	}

	//key := fdef.Uid().String32()
	key := fname
	fdef.Location = fname

	// load file to temp-file
	tempFile, err := ioutil.TempFile("", "tionde_tmp")
	if err != nil {
		return "", err
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()
	fdef.Size, err = io.Copy(tempFile, file)
	if err != nil {
		return "", fmt.Errorf("io.Copy error:%v", err)
	}

	//set upload start flag
	if err = store.Files.StartUpload(fdef); err != nil {
		log.Println("failed to create file record", fdef.Id, err)
		return "", err
	}
	// upload file to bos
	_, err = bh.svc.PutObjectFromFile(bh.conf.BucketName, key, tempFile.Name(), nil)
	if err != nil {
		store.Files.FinishUpload(fdef.Id, false, 0)
		return "", fmt.Errorf("bos.PutObject error:%v", err)
	}
	// update upload flag
	fdef, err = store.Files.FinishUpload(fdef.Id, true, fdef.Size)
	if err != nil {
		bh.svc.DeleteObject(bh.conf.BucketName, key)
		return "", err
	}

	log.Println("bos upload success ", fname, "key", key, "id", fdef.Id)

	return bh.conf.ServeURL + fname, nil
}

// Download processes request for file download.
// The returned ReadSeekCloser must be closed after use.
func (bh *bosHandler) Download(url string) (*types.FileDef, media.ReadSeekCloser, error) {
	fid := bh.GetIdFromUrl(url)
	if fid.IsZero() {
		return nil, nil, types.ErrNotFound
	}

	fd, err := bh.getFileRecord(fid)
	if err != nil {
		log.Println("Download: file not found", fid)
		return nil, nil, err
	}

	// load file to temp-file
	file, err := ioutil.TempFile("", "tionde_tmp")
	if err != nil {
		return nil, nil, err
	}
	defer os.Remove(file.Name())
	//defer file.Close()

	err = bh.svc.BasicGetObjectToFile(bh.conf.BucketName, fd.Location, file.Name())
	if err != nil {
		return nil, nil, err
	}

	return fd, file, nil
}

// Delete deletes files from aws by provided slice of locations.
func (bh *bosHandler) Delete(locations []string) error {
	_, err := bh.svc.DeleteMultipleObjectsFromKeyList(bh.conf.BucketName, locations)
	return err
}

// GetIdFromUrl converts an attahment URL to a file UID.
func (bh *bosHandler) GetIdFromUrl(url string) types.Uid {
	return media.GetIdFromUrl(url, bh.conf.ServeURL)
}

// getFileRecord given file ID reads file record from the database.
func (bh *bosHandler) getFileRecord(fid types.Uid) (*types.FileDef, error) {
	fd, err := store.Files.Get(fid.String())
	if err != nil {
		return nil, err
	}
	if fd == nil {
		return nil, types.ErrNotFound
	}
	return fd, nil
}

func init() {
	store.RegisterMediaHandler(handlerName, &bosHandler{})
}
