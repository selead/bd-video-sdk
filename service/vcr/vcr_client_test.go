package vcr

import (
	"testing"

	"github.com/spiderorg/bd-video-sdk/auth"
)

const (
	DefaultAccessKeyId     = "bbb"
	DefaultSecretAccessKey = "aaa"
)

func TestVcrClient_QueryAuditVodMediaResult(t *testing.T) {
	c, err := NewVcrClient(auth.NewBceCredentials(DefaultAccessKeyId, DefaultSecretAccessKey))
	if err != nil {
		t.Errorf("NewVcrClient failed.")
	}

	if c.GetEndpoint() != "http://vcr.bj.baidubce.com" {
		t.Errorf("GetEndpoint failed")
	}

	result, err := c.QueryAuditVodMediaResult("mda-hkfjtqsd4gdg4b20")
	if err != nil {
		t.Errorf("QueryAuditResult failed")
	}
	//println(result.Status)
	t.Log("", result)

}

func TestVcrClient_AuditVodMedia(t *testing.T) {
	c, err := NewVcrClient(auth.NewBceCredentials(DefaultAccessKeyId, DefaultSecretAccessKey))
	if err != nil {
		t.Errorf("NewVcrClient failed")
	}

	err = c.AuditVodMedia("mda-hkfjtqsd4gdg4b20", "", "")
	if err != nil {
		t.Errorf("Failed ")
	}
}
