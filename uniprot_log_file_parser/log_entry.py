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

TOOL_NAMESPACES = {
    'align',
    'blast',
    'peptidesearch',
    'uploadlists',
}

DATA_NAMESPACES = {
    'citations',
    'database',
    'diseases',
    'help',
    'keywords',
    'locations',
    'proteomes',
    'sparql',
    'taxonomy',
    'uniparc',
    'uniprot',
    'uniref',
}

NAMESPACES = TOOL_NAMESPACES | DATA_NAMESPACES


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

RESOURCE_RE = re.compile(r'GET\s(?P<resource>.*)\sHTTP/.*',
                         re.IGNORECASE | re.DOTALL)

# UNIPROTKB_ENTRY_RE = re.compile(
#     r'^/uniprot/(?P<accession>([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z]([0-9][A-Z][A-Z0-9]{2}){1,2}[0-9])(-[0-9]+)?)', re.IGNORECASE | re.DOTALL)


# ENTRY_NAMESPACES = [
#     'citations',
#     'database',
#     'diseases',
#     'keywords',
#     'locations',
#     'proteomes',
#     'taxonomy',
#     'uniparc',
#     'uniprot',
#     'uniref',
# ]

# RESOURCE_ENTRY_RE = re.compile(r'^/(' + '|'.join(ENTRY_NAMESPACES) +
#                                r')/(?P<id>[^./]+)(?P<ext>\.[^/]*)')

# RESOURCE_ENTRY_SUB_RE = re.compile(r'^/(' + '|'.join(ENTRY_NAMESPACES) +
#                                    r')/(?P<id>[^./]+)/(?P<subpage>.*)')

TOO_OLD = datetime(2002, 1, 1, 0, 0, 0, 0, pytz.UTC)

PROGRAMMATIC_APPS = {
    'Python Requests',
    'Wget',
    'Apache-HttpClient',
    'libwww-perl',
    'curl',
    'Java'
}

BROWSER_APPS = {
    'Chrome',
    'IE',
    'Firefox',
    'Opera',
    'Safari',
    'QQ Browser',
    'Edge',
    'Netscape',
    'Mobile Safari',
    'Sogou Explorer',
    'Chrome Mobile',
    'UC Browser',
    'Chromium',
    'Samsung Internet',
    'Chrome Mobile iOS',
}

NON_BOT_APPS = PROGRAMMATIC_APPS | BROWSER_APPS


class LogEntryParseError(Exception):
    pass


class LogEntryQueryError(Exception):
    pass


class LogEntry():
    def __init__(self, line):
        line = unquote(unquote(line))
        m = ENTRY_RE.match(line)
        if not m:
            raise LogEntryParseError(line)
        self.line = line
        self.date_time = m.group('date_time')
        self.user_agent = m.group('user_agent')
        self.response = m.group('response')
        self.resource = m.group('resource')
        self.bytes = m.group('bytes')
        self.referer = m.group('referer')
        self.user_agent = user_agents_parser(self.user_agent)

    def is_bot(self):
        return self.user_agent.is_bot or self.get_user_agent_browser_family() not in NON_BOT_APPS

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

    def is_request_unreasonably_old(self):
        date_time = datetime.strptime(self.date_time, '%d/%b/%Y:%H:%M:%S %z')
        return date_time < TOO_OLD

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

    def has_valid_namespace(self):
        paths = PurePosixPath(self.resource).parts
        if len(paths) > 1:
            namespace = paths[1].lower()
            return namespace in NAMESPACES
        return False

    def get_resource(self):
        m = RESOURCE_RE.match(self.resource)
        assert m, f'Cannot get parameters from {self.resource}'
        resource = m.group('resource')
        return resource
        # params = urllib.parse.urlparse(resource)
        # parsed_params = urllib.parse.parse_qs(params.query)
        # if 'query' in parsed_params:
        #     query = parsed_params['query']
        #     if len(query) != 1:
        #         raise LogEntryQueryError(self.line)
        #     return (clean(query[0]), 'query', None)
        # m = RESOURCE_ENTRY_SUB_RE.match(resource)
        # if m and m.group('id') and m.group('subpage'):
        #     return (m.group('id'), 'entry-subpage', m.group('subpage'))
        # m = RESOURCE_ENTRY_RE.match(resource)
        # if m and m.group('id'):
        #     return (m.group('id'), 'entry', None)
        # return (resource, None, None)

    def get_referer(self):
        return self.referer
        # r = urllib.parse.urlparse(self.referer)
        # return r.netloc

    def parse_resource(self):
        return urlparse(self.get_resource())

    def parse_referer(self):
        return urlparse(self.referer)

    def get_uniprot_path_info(self, parsed):
        """Connects to the next available port.

        Args:
            path: the relative path of the resource within UniProt

        Returns:
            namespace: the namespace of the resource | homepage
            resource_type: homepage | results | entry
            dataum:
                results --> query
                entry --> accession/ID
                blast|peptidesearch results --> namespace of results
        """
        if parsed.path == '/':
            return 'homepage', 'homepage', None

        parts = PurePosixPath(urlparse(parsed.path).path).parts
        datum = None
        resource_type = None
        namespace = parts[1]
        if parts[1] in TOOL_NAMESPACES:
            namespace = parts[1]
            if len(parts) == 2:
                resource_type = 'job-submission'
                # TODO: look at ?about parameter
            elif len(parts) == 3:
                resource_type = 'results'
            elif len(parts) == 4:
                resource_type = 'results'
                datum = parts[2]
        else:
            if len(parts) == 1:
                return parts
            elif len(parts) == 2:
                parsed_params = parse_qs(parsed.query)
                if 'query' in parsed_params and parsed_params['query']:
                    q = parsed_params['query']
                    datum = clean(' '.join(q))
                resource_type = 'results'
            elif len(parts) >= 3:
                datum = parts[2]
                if len(parts) == 3:
                    resource_type = 'entry'
                elif len(parts) == 4:
                    resource_type = f'entry-{parts[3]}'
                else:
                    print(parts)
                    print('more than 4')
        return namespace, resource_type, datum
