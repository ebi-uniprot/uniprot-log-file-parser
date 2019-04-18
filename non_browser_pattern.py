import json
from urllib import parse, request
import re


def fetch_json(url):
    with request.urlopen(url) as url_opened:
        data = json.loads(url_opened.read().decode())
        return data


def get_crawler_user_agents():
    CRAWLER_USER_AGENTS = 'https://raw.githubusercontent.com/monperrus/crawler-user-agents/master/crawler-user-agents.json'
    return fetch_json(CRAWLER_USER_AGENTS)


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


if __name__ == '__main__':
    print(get_non_browser_pattern())
