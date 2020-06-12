import json
from urllib import parse, request
import re


def fetch_json(url):
    with request.urlopen(url) as url_opened:
        data = json.loads(url_opened.read().decode())
        return data


def get_non_browser_pattern():
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
        r'the\sknowledge\sai',
        r'arachni',
        r'siteuptime',
        r'microsoft\soffice',
    ]
    re_non_browser_patterns = re.compile(
        '(' + '|'.join(non_browser_patterns) + ')', re.IGNORECASE)

    CRAWLER_USER_AGENTS = 'https://raw.githubusercontent.com/monperrus/crawler-user-agents/master/crawler-user-agents.json'
    crawlers = fetch_json(CRAWLER_USER_AGENTS)
    crawler_patterns = [crawler['pattern'].lower() for crawler in crawlers]

    for p in crawler_patterns:
        if re_non_browser_patterns.search(p) or p in ['[ww]get', 'livelap[bb]ot']:
            continue
        p = p.replace(r'-', r'\-') \
            .replace(r'.com', r'') \
            .replace(r'.', r'\.') \
            .replace(r'\/', r'') \
            .replace(r'(^| )', r'') \
            .replace(r'^', r'') \
            .replace(r' ', r'\s') \
            .replace(r'!', r'\!')
        non_browser_patterns.append(p)

    return '(' + '|'.join(non_browser_patterns) + ')'


if __name__ == '__main__':
    print(get_non_browser_pattern())
