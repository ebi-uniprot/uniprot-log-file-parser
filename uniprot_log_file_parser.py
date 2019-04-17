import re
import json
from urllib import parse, request
from os import path
import argparse


def fetch_json(url):
    with request.urlopen(url) as url_opened:
        data = json.loads(url_opened.read().decode())
        return data


def get_crawler_user_agents():
    CRAWLER_USER_AGENTS = 'https://raw.githubusercontent.com/monperrus/crawler-user-agents/master/crawler-user-agents.json'
    return fetch_json(CRAWLER_USER_AGENTS)


def compute_non_browser_pattern():
    non_browser_patterns = [
        r'bot',
        r'python',
        r'wget',
        r'curl',
        r'perl',
        r'java',
        r'crawler',
        r'uniprot',
        r'monitor',
        r'matlab',
        r'the\sknowledge\sai',
        r'searchhelper',
        r'winhttp',
        r'ruby',
        r'scraper',
        r'skyline',
        r'\-',
        r'nucpred'
        r'the\sknowledge\sai'
    ]
    crawler_user_agents = get_crawler_user_agents()
    re_non_browser_patterns = re.compile(
        '(' + '|'.join(non_browser_patterns) + ')', re.IGNORECASE)
    crawler_user_agent_patterns = [crawler_user_agent['pattern']
                                   for crawler_user_agent in crawler_user_agents]
    for el in crawler_user_agent_patterns:
        el = el.lower()
        if re_non_browser_patterns.search(el) or el in ['[ww]get', 'livelap[bb]ot']:
            continue
        el = el.replace(r'-', r'\-')
        el = el.replace(r'.com', r'')
        el = el.replace(r'.', r'\.')
        el = el.replace(r'\/', r'')
        el = el.replace(r'(^| )', r'')
        el = el.replace(r'^', r'')
        el = el.replace(r' ', r'\s')
        el = el.replace(r'!', r'\!')
        non_browser_patterns.append(el)

    return '(' + '|'.join(non_browser_patterns) + ')'


def get_non_browser_pattern():
    # This is just a cached result of compute_non_browser_pattern
    return r"(bot|python|wget|curl|perl|java|crawler|uniprot|monitor|matlab|the\sknowledge\sai|searchhelper|winhttp|ruby|scraper|skyline|\-|nucpredthe\sknowledge\sai|slurp|httpunit|nutch|phpcrawl|biglotron|teoma|convera|gigablast|ia_archiver|webmon\s|httrack|grub\.org|netresearchserver|speedy|fluffy|findlink|panscient|yanga|yandeximages|cyberpatrol|baiduspider|postrank|page2rss|linkdex|ezooms|heritrix|findthatfile|europarchive\.org|mappydata|eright|apercite|aboundex|summify|ec2linkfinder|facebookexternalhit|yeti|retrevopageanalyzer|sogou|wotbox|ichiro|drupact|openindexspider|gnam\sgnam\sspider|coccoc|integromedb|siteexplorer\.info|proximic|changedetection|wesee:search|360spider|cc\smetadata\sscaper|g00g1e\.net|binlar|admantx|megaindex|ltx71|bubing|qwantify|lipperhey|y\!j|addthis|screaming\sfrog\sseo\sspider|metauri|scrapy|capsulechecker|collection@infegy|deusu|sonic|sysomos|trove|deadlinkchecker|embedly|iskanie|skypeuripreview|whatsapp|electricmonk|bingpreview|yahoo\slink\spreview|daum|xenu\slink\ssleuth|pingdom|appinsights|phantomjs|jetslide|newsharecounts|barkrowler|tineye|linkarchiver|yak|digg\sdeeper|dcrawl|snacktory|ning|okhttp|nuzzel|omgili|pocketparser|yisouspider|toutiaospider|muckrack|jamie's\sspider|ahc|netcraftsurveyagent|jetty|upflow|thinklab|traackr|twurly|mastodon|http_get|brandverity|check_http|ezid|lcc\s|buck|genieo|meltwaternews|moreover|newspaper|scoutjet|sentry|seoscanners|hatena|google\sweb\spreview|adscanner|netvibes|btwebclient|disqus|feedly|fetch|fever|flamingo_searchengine|flipboardproxy|g2\sweb\sservices|vkshare|siteimprove|dareboost|miniflux|feedspot|seokicks|tracemyfile|zgrab|datafeedwatch|zabbix|axios|amazon\scloudfront|pulsepoint|wordupinfosearch|webdatastats|httpurlconnection|outbrain|w3c_validator|validator\\.nu|feedvalidator|w3c_css_validator|w3c_unicorn|blackboard|bazqux|twingly|rivva|dataprovider|grouphigh|theoldreader|anyevent|nmap\sscripting\sengine|2ip\.ru|clickagy|google\sfavicon|hubspot)"


non_browser_pattern = get_non_browser_pattern()
re_non_browser = re.compile(non_browser_pattern, re.IGNORECASE)


def is_non_browser(user_agent):
    return re_non_browser.search(user_agent)


def is_short(user_agent):
    return len(user_agent) < 40


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
                continue
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
