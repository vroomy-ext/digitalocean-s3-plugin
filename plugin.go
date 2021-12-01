package plugin

import (
	"fmt"
	"log"

	"github.com/mojura/kiroku"
	s3 "github.com/mojura/sync-s3"
	"github.com/vroomy/vroomy"
)

var p Plugin

func init() {
	if err := vroomy.Register("mojura-source", &p); err != nil {
		log.Fatal(err)
	}
}

type Plugin struct {
	vroomy.BasePlugin

	source kiroku.Source
}

// Load ensures Profiles Database is built and open for access
func (p *Plugin) Load(env map[string]string) (err error) {
	var opts s3.Options
	opts.Key = env["s3-key"]
	opts.Secret = env["s3-secret"]
	opts.Bucket = env["s3-env"]
	// Region is always us-east-1 for Digital Ocean spaces.
	opts.Region = "us-east-1"
	// Region is set as endpoint for Digital Ocean spaces.
	opts.Endpoint = fmt.Sprintf("https://%s.digitaloceanspaces.com", env["s3-region"])

	if p.source, err = s3.New(opts); err != nil {
		err = fmt.Errorf("error loading DigitalOcean S3 Client: %v", err)
		return
	}

	return
}

// Backend exposes this plugin's data layer to other plugins
func (p *Plugin) Backend() interface{} {
	return p.source
}
