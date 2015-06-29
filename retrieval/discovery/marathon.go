package discovery

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	clientmodel "github.com/prometheus/client_golang/model"
	"github.com/prometheus/log"
	"github.com/prometheus/prometheus/config"
	"github.com/rocket-internet-berlin/DISCOVERY/shared/pkg/marathon"
)

type MarathonDiscovery struct {
	marathonHost string

	done   chan struct{}
	ticker *time.Ticker
}

func NewMarathonDiscovery(conf *config.MarathonSDConfig) *MarathonDiscovery {
	return &MarathonDiscovery{
		marathonHost: conf.MarathonHost,

		done:   make(chan struct{}),
		ticker: time.NewTicker(time.Duration(conf.RefreshInterval)),
	}
}

func (d *MarathonDiscovery) Sources() []string {
	return []string{"marathon:" + d.marathonHost}
}

func (d *MarathonDiscovery) Run(ch chan<- *config.TargetGroup) {
	defer close(ch)

	d.refresh(ch)

	for {
		select {
		case <-d.ticker.C:
			if err := d.refresh(ch); err != nil {
				log.Errorf("Error refreshing Marathon targets: %s", err)
			}
		case <-d.done:
			return
		}
	}
}

func (d *MarathonDiscovery) Stop() {
	log.Debug("Stopping Marathon discovery for %s...", d.marathonHost)

	d.ticker.Stop()
	d.done <- struct{}{}

	log.Debug("Marathon discovery for %s stopped.", d.marathonHost)
}

func (d *MarathonDiscovery) refresh(ch chan<- *config.TargetGroup) error {
	apps, err := marathon.AllApps(d.marathonHost, true)
	if err != nil {
		return err
	}

	var targets []clientmodel.LabelSet
	for _, app := range apps {
		config := app.Env["PROMETHEUS_ENDPOINT"]
		if config == "" {
			continue
		}

		slashPos := strings.IndexByte(config, '/')
		if config[0] != ':' || slashPos == -1 {
			log.Error("invalid PROMETHEUS_ENDPOINT:", config)
			continue
		}

		portIndex, err := strconv.Atoi(config[1:slashPos])
		if err != nil {
			log.Error("invalid PROMETHEUS_ENDPOINT:", config)
			continue
		}

		path := config[slashPos:]

		for _, task := range app.Tasks {
			if task.StartedAt == nil {
				continue
			}

			host, port, err := task.HostAndPort(portIndex)
			if err != nil {
				log.Error(err)
				continue
			}

			target := clientmodel.LabelSet{
				clientmodel.AddressLabel:     clientmodel.LabelValue(fmt.Sprintf("%s:%d", host, port)),
				clientmodel.MetricsPathLabel: clientmodel.LabelValue(path),
				"app":     clientmodel.LabelValue(app.ID),
				"task_id": clientmodel.LabelValue(task.ID),
			}
			targets = append(targets, target)
		}
	}

	ch <- &config.TargetGroup{
		Source:  "marathon:" + d.marathonHost,
		Targets: targets,
	}

	return nil
}
