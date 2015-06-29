package discovery

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/ec2"
	clientmodel "github.com/prometheus/client_golang/model"
	"github.com/prometheus/log"
	"github.com/prometheus/prometheus/config"
)

type EC2Discovery struct {
	region string

	client *ec2.EC2
	done   chan struct{}
	ticker *time.Ticker
}

func NewEC2Discovery(conf *config.EC2SDConfig) *EC2Discovery {
	return &EC2Discovery{
		region: conf.Region,

		client: ec2.New(&aws.Config{
			Credentials: credentials.NewChainCredentials([]credentials.Provider{
				&credentials.EnvProvider{},
				&credentials.EC2RoleProvider{},
			}),
			Region: conf.Region,
		}),
		done:   make(chan struct{}),
		ticker: time.NewTicker(time.Duration(conf.RefreshInterval)),
	}
}

func (d *EC2Discovery) Sources() []string {
	return []string{"ec2:" + d.region}
}

func (d *EC2Discovery) Run(ch chan<- *config.TargetGroup) {
	defer close(ch)

	d.refresh(ch)

	for {
		select {
		case <-d.ticker.C:
			if err := d.refresh(ch); err != nil {
				log.Errorf("Error refreshing EC2 targets: %s", err)
			}
		case <-d.done:
			return
		}
	}
}

func (d *EC2Discovery) Stop() {
	log.Debug("Stopping EC2 discovery for %s...", d.region)

	d.ticker.Stop()
	d.done <- struct{}{}

	log.Debug("EC2 discovery for %s stopped.", d.region)
}

func (d *EC2Discovery) refresh(ch chan<- *config.TargetGroup) error {
	output, err := d.client.DescribeInstances(nil)
	if err != nil {
		return err
	}

	var targets []clientmodel.LabelSet
	for _, reserv := range output.Reservations {
		for _, inst := range reserv.Instances {
			if inst.PrivateIPAddress == nil {
				continue
			}
			target := clientmodel.LabelSet{
				clientmodel.AddressLabel: clientmodel.LabelValue(*inst.PrivateIPAddress + ":9100"),
				"region":                 clientmodel.LabelValue(d.region),
			}
			for _, tag := range inst.Tags {
				if *tag.Key == "Name" {
					target["name"] = clientmodel.LabelValue(*tag.Value)
					break
				}
			}
			targets = append(targets, target)
		}
	}

	ch <- &config.TargetGroup{
		Source:  "ec2:" + d.region,
		Targets: targets,
	}

	return nil
}
