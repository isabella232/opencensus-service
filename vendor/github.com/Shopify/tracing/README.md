# tracing
Go package implementing open-tracing for Shopify applications

## Usage
`shopify/tracing` implements the OpenTracing Tracer interface. To configure the global tracer:

```
import (
	"github.com/Shopify/tracing"
	"github.com/opentracing/opentracing-go"

	"golang.org/x/net/context"
)

func main() {
	tracer := tracing.New(context.Background(), tracing.WithApplicationName("<my app>"))
	opentracing.SetGlobalTracer(tracer)
}
```

Now you can trace your application with the methods from the [OpenTracing package](https://github.com/opentracing/opentracing-go)

## OpenCensus

`shopify/tracing` also provides an OpenCensus Exporter to the trace-proxy. To configure this:

```
import (
	"go.opencensus.io/trace"
)

func main() {
	e := tracing.NewExporter(context.Background(), tracing.WithApplicationName("<my app>"))
	trace.RegisterExporter(e)
}
```

Now you can trace your application with the methods from the [OpenCensus `trace` package](https://go.opencensus.io/trace)
