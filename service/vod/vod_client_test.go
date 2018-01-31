package vod

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/spiderorg/bd-video-sdk/auth"
	"github.com/spiderorg/bd-video-sdk/service/bos"
)

const (
	DefaultAccessKeyId     = "aaa"
	DefaultSecretAccessKey = "bbb"
	TestMedia              = "30.mp4"
)

func TestVodClient_CreateMedia(t *testing.T) {
	c, err := NewVodClient(auth.NewBceCredentials(DefaultAccessKeyId, DefaultSecretAccessKey))
	if err != nil {
		t.Errorf("NewVodClient failed.")
	}
	if c.GetEndpoint() != "http://vod.bj.baidubce.com" {
		t.Errorf("GetEndpoint failed")
	}

	result, err := c.ApplyMedia()
	if err != nil {
		t.Errorf("Apply media failed")
		t.Error(err.Error())
		return
	}

	t.Log("", result)

	bosClient, err := bos.NewBosClient(auth.NewBceCredentials(DefaultAccessKeyId, DefaultSecretAccessKey))
	if err != nil {
		t.Errorf("Create Bos Client failed")
	}

	file, err := os.Open(TestMedia)
	if err != nil {
		t.Errorf("Open file failed")
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	_, err = bosClient.PutObject(result.SourceBucket, result.SourceKey, bytes.NewReader(content), "", "", nil)
	if err != nil {
		t.Errorf(err.Error())
	}

	_, err = c.ProcessMedia(result.MediaId, ProcessMediaRequest{
		Title:       "test vod sdk",
		Description: "test process media",
	})
	if err != nil {
		t.Error(err.Error())
	}
}
