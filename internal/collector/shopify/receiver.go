package shopify

import (
	"context"
	"fmt"
	"strconv"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/census-instrumentation/opencensus-service/cmd/occollector/app/builder"
	"github.com/census-instrumentation/opencensus-service/consumer"
	"github.com/census-instrumentation/opencensus-service/receiver"
	"github.com/census-instrumentation/opencensus-service/receiver/shopifyreceiver"
)

// Start starts the Shopify Tracing receiver endpoint.
func Start(logger *zap.Logger, v *viper.Viper, traceConsumer consumer.TraceConsumer, asyncErrorChan chan<- error) (receiver.TraceReceiver, error) {
	rOpts, err := builder.NewDefaultShopifyReceiverCfg().InitFromViper(v)
	if err != nil {
		return nil, err
	}

	addr := ":" + strconv.FormatInt(int64(rOpts.Port), 10)
	si, err := shopifyreceiver.New(addr, traceConsumer)
	if err != nil {
		return nil, fmt.Errorf("Failed to create the Shopify receiver: %v", err)
	}

	if err := si.StartTraceReception(context.Background(), asyncErrorChan); err != nil {
		return nil, fmt.Errorf("Cannot start Shopify receiver to address %q: %v", addr, err)
	}

	logger.Info("Shopify receiver is running.", zap.Int("port", rOpts.Port))

	return si, nil
}
