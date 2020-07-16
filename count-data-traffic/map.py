#!/usr/bin/env python3
import re
from urllib import parse, request
from os import path
import argparse
from collections import defaultdict
from datetime import datetime


def get_bot_regexp():
    # This is just a cached result of  running patterns.get_user_agent_regex(includeProgrammatic=False)
    # Ie allow programmatic and browser and exclude bot-like user agents
    return r"(bot|crawler|monitor|the\sknowledge\sai|searchhelper|winhttp|scraper|skyline|\-|nucpred|the\sknowledge\sai|arachni|siteuptime|microsoft\soffice|riddler|validator|slurp|httpunit|nutch|phpcrawl|biglotron|teoma|convera|gigablast|ia_archiver|webmon\s|httrack|grub\.org|netresearchserver|speedy|fluffy|findlink|panscient|yanga|yandeximages|cyberpatrol|baiduspider|postrank|page2rss|linkdex|ezooms|heritrix|findthatfile|europarchive\.org|mappydata|eright|apercite|aboundex|summify|ec2linkfinder|facebookexternalhit|yeti|retrevopageanalyzer|sogou|wotbox|ichiro|drupact|openindexspider|gnam\sgnam\sspider|coccoc|integromedb|siteexplorer\.info|proximic|changedetection|wesee:search|360spider|cc\smetadata\sscaper|g00g1e\.net|binlar|admantx|megaindex|ltx71|bubing|qwantify|lipperhey|y\!j|addthis|screaming\sfrog\sseo\sspider|metauri|scrapy|livelap[bb]ot|capsulechecker|collection@infegy|deusu|sonic|sysomos|trove|deadlinkchecker|embedly|iskanie|skypeuripreview|whatsapp|electricmonk|bingpreview|yahoo\slink\spreview|daum|xenu\slink\ssleuth|pingdom|appinsights|phantomjs|jetslide|newsharecounts|bark[rr]owler|tineye|linkarchiver|yak|digg\sdeeper|dcrawl|snacktory|ning|okhttp|nuzzel|omgili|pocketparser|yisouspider|toutiaospider|muckrack|jamie's\sspider|ahc|netcraftsurveyagent|jetty|upflow|thinklab|traackr|twurly|mastodon|http_get|brandverity|check_http|ezid|lcc\s|buck|genieo|meltwaternews|moreover|newspaper|scoutjet|sentry|seoscanners|hatena|google\sweb\spreview|adscanner|netvibes|btwebclient|disqus|feedly|fetch|fever|flamingo_searchengine|flipboardproxy|g2\sweb\sservices|vkshare|siteimprove|dareboost|miniflux|feedspot|seokicks|tracemyfile|zgrab|datafeedwatch|zabbix|axios|amazon\scloudfront|pulsepoint|wordupinfosearch|webdatastats|httpurlconnection|outbrain|w3c_unicorn|blackboard|bazqux|twingly|rivva|dataprovider|grouphigh|theoldreader|anyevent|nmap\sscripting\sengine|2ip\.ru|clickagy|google\sfavicon|hubspot|headlesschrome|checkmarknetwork|uptime|mixnodecache|fedoraplanet|friendica|nextcloud|tiny\stiny\srss|bytespider|datanyze|trendsmapresolver|tweetedtimes|gwene|simplepie|searchatlas|superfeedr|domains\sproject|pandalytics|pagepeeker|vigil|seewithkids|yandexmetrika|yandexturbo|yandeximageresizer|yandexvideoparser|cincraw|freshrss)"


re_bot = re.compile(get_bot_regexp(), re.IGNORECASE)


def is_bot(user_agent):
    return re_bot.search(user_agent)


def is_short(user_agent):
    return len(user_agent) < 40


def is_success(response):
    return response == '200'


def contains_fil(resource):
    return '&fil=' in resource


domains = [
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

re_domain = re.compile(r'GET\s/(' + '|'.join(domains) + r')/')


def get_domain(resource):
    m = re_domain.match(resource)
    return m and m.groups() and len(m.groups()) == 1 and m.groups()[0]


def parse_log_file(log_file_path):
    re_log_line = re.compile(
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

    date_time = None
    total_requests = defaultdict(int)
    total_bytes = defaultdict(int)
    with open(log_file_path, 'r', encoding='unicode_escape') as f:
        line_number = 0
        while True:
            line_number += 1
            try:
                line = f.readline()
            except Exception as e:
                print(f'[line {line_number}]: {e}', flush=True)
            if not line:
                break
            line = parse.unquote(parse.unquote(line))
            m = re_log_line.match(line)
            if not m:
                print(f'Skipping (unable to parse): {line}', flush=True)
                continue
            user_agent = m.group('user_agent')
            response = m.group('response')
            if is_success(response) and not is_bot(user_agent):
                date_time = m.group('date_time')
                date_time = datetime.strptime(
                    date_time, '%d/%b/%Y:%H:%M:%S %z')
                date_time = date_time.strftime("%Y-%m-%d")
                try:
                    total_bytes[date_time] += int(float(m.group('bytes')))
                    total_requests[date_time] += 1
                except ValueError:
                    pass

    return total_requests, total_bytes


def get_root_out_filename(log_file_path):
    file_path, filename = path.split(log_file_path)
    _, directory = path.split(file_path)
    return f'{directory}__{filename}'


def get_csv_out_filename(log_file_path):
    root_out_filename = get_root_out_filename(log_file_path)
    return f'{root_out_filename}.csv'


def write_to_csv_file(total_requests, total_bytes, file_path):
    with open(file_path, 'w') as f:
        for date in total_requests.keys():
            print(f'{date},{total_requests[date]},{total_bytes[date]}', file=f)


def write_user_agents_too_short_to_file(user_agents_too_short, file_path):
    with open(file_path, 'w') as f:
        f.write('\n'.join(user_agents_too_short))


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('log_file_path', type=str,
                        help='The path to log file to be parsed')
    parser.add_argument('out_directory', type=str,
                        help='The path to the directory for the resulting csv file to be saved')
    args = parser.parse_args()
    return args.log_file_path, args.out_directory


def main():
    log_file_path, out_directory = get_arguments()
    print(
        f'Parsing: {log_file_path} and saving output to directory: {out_directory}', flush=True)
    total_requests, total_bytes = parse_log_file(log_file_path)
    if total_requests and total_requests:
        csv_out_filename = get_csv_out_filename(log_file_path)
        csv_file_path = path.join(out_directory, csv_out_filename)
        write_to_csv_file(total_requests, total_bytes, csv_file_path)


if __name__ == '__main__':
    main()
