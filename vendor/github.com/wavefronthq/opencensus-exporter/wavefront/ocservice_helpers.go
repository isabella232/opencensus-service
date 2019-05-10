// This file contains helpers for the exporter in opencensus-service

package wavefront

import (
	"github.com/wavefronthq/wavefront-sdk-go/application"
)

type ServiceOptions struct {
	SourceOverride    *string           `mapstructure:"override_source,omitempty"`
	ApplicationName   *string           `mapstructure:"application_name,omitempty"`
	ServiceName       *string           `mapstructure:"service_name,omitempty"`
	CustomTags        map[string]string `mapstructure:"custom_tags,omitempty"`
	MaxQueueSize      *int              `mapstructure:"max_queue_size,omitempty"`
	DisableSelfHealth *bool             `mapstructure:"disable_self_health,omitempty"`
	VerboseLogging    *bool             `mapstructure:"verbose_logging,omitempty"`
}

func WithServiceOptions(so *ServiceOptions) Option {
	return func(o *Options) {
		if so.SourceOverride != nil {
			o.Source = *so.SourceOverride
		}
		if so.ApplicationName != nil && so.ServiceName != nil {
			o.appMap = application.New(*so.ApplicationName, *so.ServiceName).Map()
		}
		if so.CustomTags != nil {
			for k, v := range so.CustomTags {
				if k != "" && v != "" {
					o.appMap[k] = v
				}
			}
		}
		if so.MaxQueueSize != nil {
			o.qSize = *so.MaxQueueSize
		}
		if so.DisableSelfHealth != nil {
			o.DisableSelfHealth = *so.DisableSelfHealth
		}
		if so.VerboseLogging != nil {
			o.VerboseLogging = *so.VerboseLogging
		}
	}
}
