import re
import json
from urllib import parse
import get_non_browser_pattern
from os import path
import argparse

non_browser_pattern = get_non_browser_pattern.get_non_browser_pattern()
re_non_browser = re.compile(non_browser_pattern, re.IGNORECASE)


def is_non_browser(user_agent):
    return re_non_browser.search(user_agent)


def is_short(user_agent):
    return len(user_agent) > 40


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
    user_agent_too_short = []
    with open(log_file_path, 'r', encoding='unicode_escape') as f:
        for line in f:
            line = parse.unquote(parse.unquote(line))
            m = re_log_line.match(line)
            if not m:
                continue
            user_agent = m.group('user_agent')
            response = m.group('response')
            resource = m.group('resource')
            if is_non_browser(user_agent):
                continue
            if is_short(user_agent):
                user_agent_too_short.append(user_agent)
            if is_success(response) and is_query(resource) and not contains_fil(resource):
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

    return browser_requests, user_agent_too_short


def get_root_out_filename(log_file_path):
    file_path, filename = path.split(log_file_path)
    _, directory = path.split(file_path)
    return f'{directory}__{filename}'


def get_json_out_filename(log_file_path):
    root_out_filename = get_root_out_filename(log_file_path)
    return f'{root_out_filename}.json'


def get_too_short_out_filename(log_file_path):
    root_out_filename = get_root_out_filename(log_file_path)
    return f'{root_out_filename}.user_agent_too_short.txt'


def write_to_json_file(obj, file_path):
    with open(file_path, 'w') as f:
        json.dump(obj, f, indent=4)


def write_user_agent_too_short_to_file(user_agent_too_short, file_path):
    with open(file_path, 'w') as f:
        f.write('\n'.join(user_agent_too_short))


def main(log_file_path, out_directory):
    valid_browser_requests, user_agent_too_short = parse_log_file(
        log_file_path)

    json_out_filename = get_json_out_filename(log_file_path)
    json_file_path = path.join(out_directory, json_out_filename)
    write_to_json_file(valid_browser_requests, json_file_path)

    too_short_out_filename = get_too_short_out_filename(log_file_path)
    too_short_out_path = path.join(
        out_directory, too_short_out_filename)
    write_user_agent_too_short_to_file(
        user_agent_too_short, too_short_out_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('log_file_path', type=str,
                        help='The path to log file to be parsed')
    parser.add_argument('out_directory', type=str,
                        help='The path to the directory for the resulting json file to be saved')
    args = parser.parse_args()
    main(args.log_file_path, args.out_directory)
