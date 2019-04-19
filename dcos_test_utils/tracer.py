from opencensus.common.transports import sync
from opencensus.trace import span_data
from opencensus.trace import config_integration, file_exporter
from opencensus.trace.propagation.trace_context_http_header_format import TraceContextPropagator
from opencensus.trace.tracer import Tracer
from opencensus.ext.zipkin.trace_exporter import ZipkinExporter
from contextlib import contextmanager
import json
import subprocess


_tracer = None
_tracing_disabled = False
_tags = {}
subprocess_run = subprocess.run


class FileExporter(file_exporter.FileExporter):
    """
    Write traces to a file - used for unit testing.
    """
    def emit(self, span_datas):
        with open(self.file_name, self.file_mode) as file:
            legacy_trace_json = span_data.format_legacy_trace_json(span_datas)
            file.write(json.dumps(legacy_trace_json) + "\n")


def traced_subprocess_run(*args, **kwargs):
    """
    Hook subprocess.run for tracing.
    """
    if _tracer is None:
        return subprocess_run(*args, **kwargs)

    with _tracer.span('subprocess') as span:
        span.add_attribute('subprocess.command', subprocess.list2cmdline(args[0]))
        ret = subprocess_run(*args, **kwargs)
        span.add_attribute('subprocess.status', str(ret.returncode))
        return ret


def tracer(disable_tracing=False, zipkin_endpoint=None, trace_tags=None):
    """
    Initialize distributed tracing - if zipkin_endpoint is not specified, traces will be written to
    traces.json.
    """
    global _tracer
    global _tracing_disabled
    global _tags

    if _tracer is not None:
        return _tracer

    if disable_tracing is True or _tracing_disabled:
        _tracing_disabled = True
        return

    config_integration.trace_integrations(['requests'])

    if zipkin_endpoint:
        exporter = ZipkinExporter(service_name='pytest',
                                  host_name=zipkin_endpoint,
                                  port=9411,
                                  endpoint='/api/v2/spans')
    else:
        exporter = FileExporter('traces.json', file_mode='a+')

    if trace_tags:
        for tag in trace_tags:
            if '=' not in tag:
                continue

            tag = tag.split('=', 1)
            _tags[tag[0]] = tag[1]

    _tracer = Tracer(propagator=TraceContextPropagator(), exporter=exporter)
    subprocess.run = traced_subprocess_run
    return _tracer


@contextmanager
def start_span(name):
    """
    Create a new tracing span.
    """
    if _tracer and _tracer is not True:
        with _tracer.span(name) as span:
            for k, v in _tags.items():
                span.add_attribute(k, v)
            yield span
    else:
        yield None
