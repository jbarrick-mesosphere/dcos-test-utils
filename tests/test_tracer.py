"""Tests for dcos_test_utils.tracer."""
import os
import json
import threading
from dcos_test_utils import helpers, tracer


def test_tracing(testdir):
    expected_traces = [
        {
            'name': '[requests]GET',
            'parent': 'test_http_trace',
            'attrs': {'http.status_code': '200', 'http.url': 'https://google.com/'},
        },
        {
            'name': '[requests]GET',
            'parent': 'test_http_trace',
            'attrs': {'http.status_code': '200', 'http.url': 'https://google.com/'},
        },
        {
            'name': 'test_http_trace',
            'attrs': {'pytest.result': 'passed', 'mytag': 'abcd', 'othertag': 'def'},
        },
        {
            'name': '[requests]GET',
            'parent': 'test_http_trace_new_test',
            'attrs': {'http.status_code': '200', 'http.url': 'https://google.com/'},
        },
        {
            'name': 'test_http_trace_new_test',
            'attrs': {'pytest.result': 'passed', 'mytag': 'abcd', 'othertag': 'def'},
        },
        {
            'name': 'subprocess',
            'parent': 'test_subprocess_tracing',
            'attrs': {'subprocess.command': '/bin/bash -c "sleep 1"', 'subprocess.status': '0'},
        },
        {
            'name': 'test_subprocess_tracing',
            'attrs': {'pytest.result': 'passed', 'mytag': 'abcd', 'othertag': 'def'},
        },
        {
            'name': 'subprocess',
            'parent': 'test_subprocess_tracing_error',
            'attrs': {'subprocess.command': '/bin/bash -c "exit 1"', 'subprocess.status': '1'},
        },
        {
            'name': 'test_subprocess_tracing_error',
            'attrs': {'pytest.result': 'failed', 'mytag': 'abcd', 'othertag': 'def'},
        },
        {
            'name': 'test_xfail_fails',
            'attrs': {'pytest.result': 'skipped', 'mytag': 'abcd', 'othertag': 'def'},
        },
        {
            'name': 'test_xfail_passes',
            'attrs': {'pytest.result': 'passed', 'mytag': 'abcd', 'othertag': 'def'},
        },

    ]

    # Reset the global tracer - since pytest uses the same process.
    tracer._tracer = None
    tracer._tracing_disabled = False

    # run a suite of tests using the pytest plugin.
    testdir.makepyfile("""
from dcos_test_utils import helpers, tracer
import pook
import pytest
import subprocess
import sys

def test_http_trace():
    with pook.use():
        pook.get('https://google.com/', reply=200)
        pook.get('https://google.com/', reply=200)

        sess = helpers.ApiClientSession(helpers.Url.from_string("https://google.com"))
        sess.get("/")
        sess.get("/")


def test_http_trace_new_test():
    with pook.use():
        pook.get('https://google.com/', reply=200)

        sess = helpers.ApiClientSession(helpers.Url.from_string("https://google.com"))
        sess.get("/")


def test_subprocess_tracing():
    subprocess.run(['/bin/bash', '-c', 'sleep 1'])


def test_subprocess_tracing_error():
    subprocess.run(['/bin/bash', '-c', 'exit 1'])
    assert False


@pytest.mark.xfailflake(
    jira='DCOS-1337',
    reason='A reason',
    since='2019-01-25'
)
def test_xfail_fails():
    assert False


@pytest.mark.xfailflake(
    jira='DCOS-1337',
    reason='A reason',
    since='2019-01-25'
)
def test_xfail_passes():
    assert True
""")
    testdir.runpytest('--trace-tags=mytag=abcd', '--trace-tags=othertag=def')

    # Load traces from traces.json
    actual_traces = []
    span_ids = {}
    trace_ids = set()

    for line in open(os.path.join(str(testdir), 'traces.json')):
        span = json.loads(line)
        assert len(span['spans']) == 1
        span_ids[span['spans'][0]['spanId']] = span['spans'][0]
        actual_traces.append(span['spans'][0])
        trace_ids.add(span['traceId'])

    # Validate the traces
    for index, span in enumerate(actual_traces):
        # validate trace name
        assert span['displayName']['value'] == expected_traces[index]['name']

        # validate trace parent
        if expected_traces[index].get('parent'):
            assert 'parentSpanId' in span
            assert span_ids[span['parentSpanId']]['displayName']['value'] == expected_traces[index]['parent']
        else:
            assert 'parentSpanId' not in span

        # validate trace attributes
        expected_attrs = expected_traces[index]['attrs']

        assert 'attributes' in span

        attrs = {}
        for attr, item in span['attributes']['attributeMap'].items():
            attrs[attr] = item['string_value']['value']

        assert attrs == expected_traces[index]['attrs']

    # ensure that they all have the same trace id
    assert len(trace_ids) == 1
