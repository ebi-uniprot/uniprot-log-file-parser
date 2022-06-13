#!/usr/bin/env python3
import re
from urllib.parse import unquote, parse_qs, urlparse
from pathlib import Path
from datetime import datetime
import pytz
from user_agents import parse as user_agents_parser
import sys


ENTRY_RE = re.compile(
    r"^(?P<ip1>.*?) "
    r"(?P<unknown1>.*?) "
    r"(?P<unknown2>.*?) "
    r"\[(?P<datetime>.*?)\] "
    r"\"(?P<resource>.*((HTTP\/[0-9\.]+)|null)?)\s*\" "
    r"(?P<response>\d+) "
    r"(?P<bytes>.*?) "
    r"\"(?P<referer>.*)\" "
    r"\"(?P<user_agent>.*?)\" "
    r"(?P<response_time>[0-9\.]+) "
    r"(?P<unknown4>.*?) "
    r"(?P<content_type>.*?) "
    r"(?P<ip2>.*?) "
    r"(?P<unknown5>.*?)$",
    re.DOTALL,
)

RESOURCE_RE = re.compile(r"(?P<method>\w+)\s(?P<resource>.*)\sHTTP/.*", re.DOTALL)

STATIC_RE = re.compile(
    r"(/(images|scripts|style))|(\.(ico|css|png|jpg|svg|js|woff))|opensearch.xml"
)

TOO_OLD = datetime(2002, 1, 1, 0, 0, 0, 0, pytz.UTC)

PROGRAMMATIC_APPS = {
    "Python Requests",
    "Wget",
    "Apache-HttpClient",
    "libwww-perl",
    "curl",
    "Java",
}

BROWSER_APPS = {
    "Chrome",
    "IE",
    "Firefox",
    "Opera",
    "Safari",
    "QQ Browser",
    "Edge",
    "Netscape",
    "Mobile Safari",
    "Sogou Explorer",
    "Chrome Mobile",
    "UC Browser",
    "Chromium",
    "Samsung Internet",
    "Chrome Mobile iOS",
    "Chrome Mobile WebView",
    "Yandex Browser" "Thunderbird",
    "Camino",
    "Firefox Mobile",
    "Vivaldi",
    "Opera Mobile",
    "Opera Mini",
}

NON_BOT_APPS = PROGRAMMATIC_APPS | BROWSER_APPS


class LogEntryParseError(Exception):
    pass


class LogEntryQueryError(Exception):
    pass


class LogEntry:
    def __init__(self, line):
        line = unquote(unquote(line))
        m = ENTRY_RE.match(line)
        if not m:
            raise LogEntryParseError(line)
        self.line = line
        self.ip = m.group("ip1")
        self.datetime = datetime.strptime(m.group("datetime"), "%d/%b/%Y:%H:%M:%S %z")
        self.user_agent = m.group("user_agent")
        self.response = int(m.group("response"))
        self.resource = m.group("resource")
        self.bytes = m.group("bytes")
        self.referer = m.group("referer")
        self.response_time = m.group("response_time")
        self.user_agent_parsed = user_agents_parser(self.user_agent)

    def is_bot(self):
        return self.user_agent.is_bot

    def is_get(self):
        return self.resource.lower().startswith("get")

    def is_unknown_agent(self):
        return self.get_user_agent_browser_family() == "Other"

    def get_ip(self):
        return self.ip

    def get_user_agent_browser_family(self):
        return self.user_agent_parsed.browser.family

    def get_user_agent(self):
        return self.user_agent

    def is_static(self):
        return bool(STATIC_RE.search(self.resource))

    def is_opensearch(self):
        return "opensearch.xml" in self.resource

    def is_browser(self):
        return self.get_user_agent_browser_family() in BROWSER_APPS

    def is_api(self):
        return self.get_user_agent_browser_family() in PROGRAMMATIC_APPS

    def get_user_type(self):
        if self.get_user_agent_browser_family() == "Other":
            return "unknown"
        if self.is_bot():
            return "bot"
        if self.is_api():
            return "programmatic"
        if self.is_browser():
            return "browser"
        return "unknown"

    def get_response_code(self):
        return self.response

    def is_success(self):
        try:
            return int(self.response) == 200
        except ValueError as e:
            print(self.line, e, flush=True, file=sys.stderr)
            return False
        except AttributeError as e:
            print(self.line, e, flush=True, file=sys.stderr)
            return False

    def is_request_unreasonably_old(self):
        return self.datetime < TOO_OLD

    def get_date_string(self):
        return self.datetime.strftime("%Y%m%d-%H%M")

    def get_timestamp(self):
        return int(self.datetime.timestamp())

    def get_response_time(self):
        return float(self.response_time)

    def get_bytes(self):
        try:
            if self.bytes == "-":
                return None
            return int(float(self.bytes))
        except ValueError as e:
            print(self.line, e, flush=True, file=sys.stderr)

    @staticmethod
    def get_namespace(resource):
        p = re.compile(r"^/(?P<namespace>[^\.\s]+?)(/|$)")
        m = p.match(resource)
        if m:
            return m.group("namespace")
        return "root"

    def get_method_resource(self):
        m = RESOURCE_RE.match(self.resource)
        assert m, f"Cannot get parameters from {self.resource}"
        resource = m.group("resource")
        method = m.group("method")
        return method, resource

    def get_referer(self):
        if self.referer == "-":
            return None
        return self.referer
