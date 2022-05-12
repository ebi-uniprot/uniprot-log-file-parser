#!/usr/bin/env python3
import re
from urllib.parse import unquote, parse_qs, urlparse
from pathlib import PurePosixPath
from datetime import datetime
import pytz
from user_agents import parse as user_agents_parser
import sys

from .patterns import BOT_RE, PROGRAMMATIC_RE, UNKNOWN_RE
from .utils import clean

TOOL_NAMESPACES = {"align", "blast", "peptidesearch", "uploadlists", "mapping"}

DATA_NAMESPACES = {
    "citations",
    "database",
    "diseases",
    "docs",
    "program",
    "help",
    "keywords",
    "locations",
    "proteomes",
    "sparql",
    "taxonomy",
    "uniparc",
    "uniprot",
    "uniref",
    "unirule",
}

OTHER_NAMESPACES = {
    "feedback",
    "downloads",
    "core",
    "news",
}

NAMESPACES = TOOL_NAMESPACES | DATA_NAMESPACES | OTHER_NAMESPACES


ENTRY_RE = re.compile(
    r"^(?P<ip1>.*?) "
    r"(?P<unknown1>.*?) "
    r"(?P<unknown2>.*?) "
    r"\[(?P<datetime>.*?)\] "
    r"\"(?P<resource>.*((HTTP\/[0-9\.]+)|null)?)\s*\" "
    r"(?P<response>.*?) "
    r"(?P<bytes>.*?) "
    r"\"(?P<referer>.*?)\" "
    r"\"(?P<user_agent>.*?)\" "
    r"(?P<response_time>.*?) "
    r"(?P<unknown4>.*?) "
    r"(?P<content_type>.*?) "
    r"(?P<ip2>.*?) "
    r"(?P<unknown5>.*?)$",
    re.DOTALL,
)

RESOURCE_RE = re.compile(
    r"(?P<method>\w+)\s(?P<resource>.*)\sHTTP/.*", re.IGNORECASE | re.DOTALL
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
        self.response = m.group("response")
        self.resource = m.group("resource")
        self.bytes = m.group("bytes")
        self.referer = m.group("referer")
        self.user_agent = user_agents_parser(self.user_agent)

    def is_bot(self):
        return (
            self.user_agent.is_bot
            or self.get_user_agent_browser_family() not in NON_BOT_APPS
        )

    def is_get(self):
        return self.resource.lower().startswith("get")

    def is_unknown_agent(self):
        return self.get_user_agent_browser_family() == "Other"

    def get_ip(self):
        return self.ip

    def get_user_agent_browser_family(self):
        return self.user_agent.browser.family

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

    def get_bytes(self):
        try:
            if self.bytes == "-":
                return 0
            return int(float(self.bytes))
        except ValueError as e:
            print(self.line, e, flush=True, file=sys.stderr)

    def has_valid_namespace(self):
        paths = PurePosixPath(self.resource).parts
        if len(paths) > 1:
            namespace = paths[1].lower()
            return namespace in NAMESPACES
        return False

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
        # r = urllib.parse.urlparse(self.referer)
        # return r.netloc

    # def get_field_names(self, query):
    #     return Counter(set(re.findall(FIELD_NAME_RE, query)))

    def get_uniprot_namespace(self, resource):
        parsed = urlparse(resource)
        if parsed.path in ["/", ""]:
            return "homepage"
        parts = PurePosixPath(urlparse(parsed.path).path).parts
        if len(parts):
            return parts[1]
        return None
