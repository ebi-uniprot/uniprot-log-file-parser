import re
import json
from urllib import parse
import get_non_browser_pattern
from os import path


non_browser_pattern = get_non_browser_pattern.get_non_browser_pattern()
re_non_browser = re.compile(non_browser_pattern, re.IGNORECASE)


def is_browser(user_agent):
    return len(user_agent) > 50 and not re_non_browser.search(user_agent)


def is_success(response):
    return response == '200'


def contains_fil(resource):
    return '&fil=' in resource


def is_query(resource):
    return resource.startswith('GET /uniprot/?query=')


def parse_log_file(log_file_path):
    re_log_line = re.compile(
        r'^(?P<ip1>.*?) '
        r'(?P<unknown1>.*?) '
        r'(?P<unknown2>.*?) '
        r'\[(?P<date_time>.*?)\] '
        r'\"(?P<resource>.*?)\" '
        r'(?P<response>.*?) '
        r'(?P<unknown3>.*?) '
        r'\"(?P<referer>.*?)\" '
        r'\"(?P<user_agent>.*?)\" '
        r'(?P<response_time>.*?) '
        r'(?P<unknown4>.*?) '
        r'(?P<content_type>.*?) '
        r'(?P<ip2>.*?) '
        r'(?P<unknown5>.*?)$',
        re.DOTALL
    )

    browser_requests = []
    with open(log_file_path, 'r', encoding='unicode_escape') as f:
        for line in f:
            line = parse.unquote(parse.unquote(line))
            m = re_log_line.match(line)
            if not m:
                continue
            user_agent = m.group('user_agent')
            response = m.group('response')
            resource = m.group('resource')
            if is_browser(user_agent) and is_success(response) and is_query(resource) and not contains_fil(resource):
                ip = m.group('ip1')
                date_time = m.group('date_time')
                referer = m.group('referer')
                browser_requests.append({
                    'ip': ip,
                    'date_time': date_time,
                    'resource': resource,
                    'referer': referer,
                    'user_agent': user_agent,
                })

    return browser_requests


def write_to_json_file(obj, json_file_path):
    with open(json_file_path, 'w') as f:
        json.dump(obj, f, indent=4)


def main():
    log_file_path = '/nfs/public/rw/homes/tc_uni01/uuw-stats/logs/wp-p1m-cd/access_2019-03-17.log'
    browser_requests = parse_log_file(log_file_path)
    log_filename = path.basename(log_file_path)
    json_file_path = path.join('/homes/dlrice/', log_filename)
    write_to_json_file(browser_requests, json_file_path)


if __name__ == '__main__':
    main()
