package plugin

import (
	"errors"
	"fmt"
	"log"
	"strconv"

	"github.com/mojura/kiroku"
	s3 "github.com/mojura/sync-s3"
	"github.com/vroomy/vroomy"
)

var p Plugin

var (
	// ErrEmptyS3Key is returned when an s3 key is empty within the vroomy config
	ErrEmptyS3Key = errors.New("invalid s3-key, cannot be empty")
	// ErrEmptyS3Secret is returned when an s3 secret is empty within the vroomy config
	ErrEmptyS3Secret = errors.New("invalid s3-secret, cannot be empty")
	// ErrEmptyS3Env is returned when an s3 env is empty within the vroomy config
	ErrEmptyS3Env = errors.New("invalid s3-env, cannot be empty")
)

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
func (p *Plugin) Load(env vroomy.Environment) (err error) {
	var opts s3.Options
	if opts.Key = env["s3-key"]; len(opts.Key) == 0 {
		err = ErrEmptyS3Key
		return
	}

	if opts.Secret = env["s3-secret"]; len(opts.Key) == 0 {
		err = ErrEmptyS3Secret
		return
	}

	if opts.Bucket = env["s3-env"]; len(opts.Key) == 0 {
		err = ErrEmptyS3Env
		return
	}

	var maxRatePerSecond int64
	if rate := env["s3-max-rate-per-second"]; len(rate) > 0 {
		if maxRatePerSecond, err = strconv.ParseInt(rate, 10, 64); err != nil {
			return
		}
	}

	// Region is always us-east-1 for Digital Ocean spaces.
	opts.Region = "us-east-1"
	// Region is set as endpoint for Digital Ocean spaces.
	opts.Endpoint = fmt.Sprintf("https://%s.digitaloceanspaces.com", env["s3-region"])
	opts.AvoidBucketCreation = true
	opts.MaxRatePerSecond = maxRatePerSecond
	if p.source, err = s3.New(opts); err != nil {
		err = fmt.Errorf("error loading Digital Ocean s3 client: %v", err)
		return
	}

	return
}

// Backend exposes this plugin's data layer to other plugins
func (p *Plugin) Backend() interface{} {
	return p.source
}
