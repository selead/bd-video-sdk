package vodpro

import (
	"fmt"
	"testing"

	"github.com/spiderorg/bd-video-sdk/auth"
)

const (
	DefaultAccessKeyId     = "aaaa"
	DefaultSecretAccessKey = "bbbbb"
	TestMedia              = "small.mp4"
)

func TestVodproClient_CreateMedia(t *testing.T) {
	c, err := NewVodproClient(auth.NewBceCredentials(DefaultAccessKeyId, DefaultSecretAccessKey))
	if err != nil {
		t.Errorf("NewVodproClient failed: %v", err)
	}
	if c.GetEndpoint() != "http://vodpro.bj.baidubce.com" {
		t.Errorf("GetEndpoint failed")
	}
	response, err := c.CreateMedia("jianbin", "test", CreateMediaRequest{
		Path: "aa/small.mp4",
	})
	if err != nil {
		t.Errorf("Create media failed:%v", err)
		return
	}
	fmt.Println(response)
}
