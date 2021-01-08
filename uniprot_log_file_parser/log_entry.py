#!/usr/bin/env python3
import re
import urllib.parse
from pathlib import PurePosixPath
from datetime import datetime
from user_agents import parse as user_agents_parser
import sys

from .patterns import BOT_RE, PROGRAMMATIC_RE, UNKNOWN_RE
from .utils import clean

NAMESPACES = [
    'uniprot',
    'uniref',
    'uniparc',
    'taxonomy',
    'proteomes',
    'citations',
    'taxonomy',
    'locations',
    'database',
    'diseases',
    'keywords',
]


ENTRY_RE = re.compile(
    r'^(?P<ip1>.*?) '
    r'(?P<unknown1>.*?) '
    r'(?P<unknown2>.*?) '
    r'\[(?P<date_time>.*?)\] '
    r'\"(?P<resource>.*((HTTP\/[0-9\.]+)|null)?)\s*\" '
    r'(?P<response>.*?) '
    r'(?P<bytes>.*?) '
    r'\"(?P<referer>.*?)\" '
    r'\"(?P<user_agent>.*?)\" '
    r'(?P<response_time>.*?) '
    r'(?P<unknown4>.*?) '
    r'(?P<content_type>.*?) '
    r'(?P<ip2>.*?) '
    r'(?P<unknown5>.*?)$',
    re.DOTALL
)

PARAMS_RE = re.compile(r'GET\s(?P<params>.*)\sHTTP/.*',
                       re.IGNORECASE | re.DOTALL)


class LogEntryParseError(Exception):
    pass


class LogEntryQueryError(Exception):
    pass


class LogEntry():
    def __init__(self, line):
        line = urllib.parse.unquote(urllib.parse.unquote(line))
        m = ENTRY_RE.match(line)
        if not m:
            raise LogEntryParseError(line)
        self.line = line
        self.date_time = m.group('date_time')
        self.user_agent = m.group('user_agent')
        self.response = m.group('response')
        self.resource = m.group('resource')
        self.bytes = m.group('bytes')
        self.user_agent = user_agents_parser(self.user_agent)

    def is_bot(self):
        return self.user_agent.is_bot

    def is_get(self):
        return self.resource.lower().startswith('get')

    def is_unknown_agent(self):
        return self.get_user_agent_browser_family() == 'Other'

    def get_user_agent_browser_family(self):
        return self.user_agent.browser.family

    def is_success(self):
        try:
            return int(self.response) == 200
        except ValueError as e:
            print(self.line, e, flush=True, file=sys.stderr)
            return False
        except AttributeError as e:
            print(self.line, e, flush=True, file=sys.stderr)
            return False

    def query_has_facets(self):
        """
        &fil is used when the facets are activated. Eg https://www.uniprot.org/uniprot/?query=a4&fil=reviewed%3Ayes&sort=score means that the user has clicked on the reviewed facet after searching for a4
        """
        return '&fil=' in self.resource

    def get_yyyy_mm_dd(self):
        date_time = datetime.strptime(self.date_time, '%d/%b/%Y:%H:%M:%S %z')
        return date_time.strftime("%Y-%m-%d")

    def get_bytes(self):
        try:
            if self.bytes == '-':
                return 0
            return int(float(self.bytes))
        except ValueError as e:
            print(self.line, e, flush=True, file=sys.stderr)

    def get_namespace(self):
        paths = PurePosixPath(self.resource).parts
        if len(paths) > 1:
            namespace = paths[1].lower()
            if namespace in NAMESPACES:
                return namespace

    def get_query(self):
        m = PARAMS_RE.match(self.resource)
        assert m, f'Cannot get parameters from {self.resource}'
        params = m.group('params')
        params = urllib.parse.urlparse(params)
        parsed_params = urllib.parse.parse_qs(params.query)
        if 'query' in parsed_params:
            query = parsed_params['query']
            if len(query) != 1:
                raise LogEntryQueryError(self.line)
            return clean(query[0])
