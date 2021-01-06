#!/usr/bin/env python3
import re
import urllib.parse
from pathlib import PurePosixPath
from datetime import datetime
from ua_parser import user_agent_parser
import sys

from .patterns import BOT_RE, PROGRAMMATIC_RE, UNKNOWN_RE
from .utils import clean

DOMAINS = [
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
    'mapping'
]


ENTRY_RE = re.compile(
    r'^(?P<ip1>.*?) '
    r'(?P<unknown1>.*?) '
    r'(?P<unknown2>.*?) '
    r'\[(?P<date_time>.*?)\] '
    r'\"(?P<resource>.*?)\" '
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


class LogEntry():
    def __init__(self, line):
        line = urllib.parse.unquote(urllib.parse.unquote(line))
        m = ENTRY_RE.match(line)
        if not m:
            return

        self.date_time = m.group('date_time')
        self.user_agent = m.group('user_agent')
        self.response = m.group('response')
        self.resource = m.group('resource')
        self.bytes = m.group('bytes')

    def is_bot(self):
        return BOT_RE.search(self.user_agent)

    def is_get(self):
        return self.resource.lower().startswith('get')

    def is_unknown_agent(self):
        return UNKNOWN_RE.search(self.user_agent)

    def is_programmatic(self):
        return PROGRAMMATIC_RE.search(self.user_agent)

    def get_user_type(self):
        if self.is_bot():
            return 'bot'
        if self.is_unknown_agent():
            return 'unknown'
        if self.is_programmatic():
            return 'programmatic'
        return 'browser'

    def get_user_agent(self):
        return self.user_agent

    def get_user_agent_family(self):
        ua = user_agent_parser.Parse(self.user_agent)
        return ua['user_agent']['family']

    def is_success(self):
        try:
            return int(self.response) == 200
        except ValueError as e:
            print(e, flush=True, file=sys.stderr)
            return False

    def query_contains_fil(self):
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
            print(e, flush=True, file=sys.stderr)

    def get_domain(self):
        paths = PurePosixPath(self.resource).parts
        if len(paths) > 1:
            domain = paths[1].lower()
            if domain in DOMAINS:
                return domain

    def get_query(self):
        m = PARAMS_RE.match(self.resource)
        assert m, f'Cannot get parameters from {self.resource}'
        params = m.group('params')
        params = urllib.parse.urlparse(params)
        parsed_params = urllib.parse.parse_qs(params.query)
        if 'query' in parsed_params:
            query = parsed_params['query']
            assert len(query) == 1
            return clean(query[0])
