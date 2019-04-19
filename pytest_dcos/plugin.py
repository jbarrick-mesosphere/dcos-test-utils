import logging
import datetime
import json
import os
import pytest

from typing import Any
from _pytest.python import Function
from dcos_test_utils import dcos_api, enterprise, logger, tracer

logger.setup(os.getenv('LOG_LEVEL', 'DEBUG'))


@pytest.fixture(scope='session')
def dcos_api_session_factory():
    is_enterprise = os.getenv('DCOS_ENTERPRISE', 'false').lower() == 'true'

    if is_enterprise:
        return enterprise.EnterpriseApiSession
    else:
        return dcos_api.DcosApiSession


@pytest.fixture(scope='session')
def dcos_api_session(dcos_api_session_factory):
    api = dcos_api_session_factory.create()
    api.wait_for_dcos()
    return api


def _iter_xfail_markers(item):
    xfailflake_markers = [
        marker for marker in item.iter_markers() if marker.name == 'xfailflake'
    ]
    for xfailflake_marker in xfailflake_markers:
        assert 'reason' in xfailflake_marker.kwargs
        assert 'jira' in xfailflake_marker.kwargs
        assert xfailflake_marker.kwargs['jira'].startswith('DCOS')

        yield xfailflake_marker


def _write_xfailflake_report(tests):
    """
    Writes a report of all xfailflake tagged tests to the current directory.
    """
    report = []

    for test in tests:
        for xfailflake_marker in _iter_xfail_markers(test):
            report.append({
                "name": test.name,
                "module": test.module.__name__,
                "path": test.module.__file__,
                "xfailflake": xfailflake_marker.kwargs
            })

    json.dump(report, open('xfailflake.json', 'w'))


def pytest_addoption(parser):
    parser.addoption("--xfailflake-report", action="store_true",
                     help="Write a report of all tests marked flakey using the xfailflake marker to xfailflake.json.")
    parser.addoption("--disable-tracing", action="store_true", help="If set, dcos-test-utils will not attempt to export a trace of requests made.")
    parser.addoption("--zipkin", default="", help="Address of Zipkin API to export traces to.")
    parser.addoption("--trace-tags", action="append", help="Tags to append to traces.", required=False)


def pytest_collection_modifyitems(session, config, items):
    if not config.getoption("--xfailflake-report"):
        return

    _write_xfailflake_report(items)


def _add_xfail_markers(item: Function) -> None:
    """
    Mute flaky Integration Tests with custom pytest marker.
    Rationale for doing this is mentioned at DCOS-45308.
    """
    xfailflake_markers = [
        marker for marker in item.iter_markers() if marker.name == 'xfailflake'
    ]
    for xfailflake_marker in xfailflake_markers:
        assert 'reason' in xfailflake_marker.kwargs
        assert 'jira' in xfailflake_marker.kwargs
        assert xfailflake_marker.kwargs['jira'].startswith('DCOS')
        # Show the JIRA in the printed reason.
        xfailflake_marker.kwargs['reason'] = '{jira} - {reason}'.format(
            jira=xfailflake_marker.kwargs['jira'],
            reason=xfailflake_marker.kwargs['reason'],
        )
        date_text = xfailflake_marker.kwargs['since']
        try:
            datetime.datetime.strptime(date_text, '%Y-%m-%d')
        except ValueError:
            message = (
                'Incorrect date format for "since", should be YYYY-MM-DD'
            )
            raise ValueError(message)

        # The marker is not "strict" unless that is explicitly stated.
        # That means that by default, no error is raised if the test passes or
        # fails.
        strict = xfailflake_marker.kwargs.get('strict', False)
        xfailflake_marker.kwargs['strict'] = strict
        xfail_marker = pytest.mark.xfail(
            *xfailflake_marker.args,
            **xfailflake_marker.kwargs,
        )
        item.add_marker(xfail_marker)


def pytest_runtest_setup(item: Any) -> None:
    _add_xfail_markers(item)


def pytest_configure(config):
    """
    Initialize the distributed tracer with the desired settings.
    """
    tracer.tracer(config.getoption('--disable-tracing'), config.getoption('--zipkin'), config.getoption('--trace-tags'))


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """
    Attach a test result to the test item so that trace_tests can add it as an attribute.
    """
    outcome = yield

    result = outcome.get_result()

    setattr(item, 'test_status', {
        "result": result.outcome,
    })


@pytest.fixture(autouse=True)
def trace_tests(request):
    """
    A fixture that is automatically added to tests to start a trace. All spans created inside of the test will be children
    of the span created here.
    """
    with tracer.start_span(request.node.name) as span:
        yield

        if not span:
            return

        span.add_attribute('pytest.result', request.node.test_status['result'])
