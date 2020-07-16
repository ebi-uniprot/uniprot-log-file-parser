import json
from urllib import parse, request
import re


def fetch_json(url):
    with request.urlopen(url) as url_opened:
        data = json.loads(url_opened.read().decode())
        return data


def get_crawler_user_agents_regexs():
    CRAWLER_USER_AGENTS = 'https://raw.githubusercontent.com/monperrus/crawler-user-agents/master/crawler-user-agents.json'
    crawlers = fetch_json(CRAWLER_USER_AGENTS)
    return [crawler['pattern'].lower() for crawler in crawlers]


def get_user_agent_regex(includeProgrammatic=True):
    regexs = [
        r'bot',
        r'crawler',
        r'monitor',
        r'the\sknowledge\sai',
        r'searchhelper',
        r'winhttp',
        r'scraper',
        r'skyline',
        r'\-',
        r'nucpred',
        r'the\sknowledge\sai',
        r'arachni',
        r'siteuptime',
        r'microsoft\soffice',
        r'riddler',
        r'validator',
    ]

    programmatic_regexs = [
        r'[ww]get',
        r'python-urllib',
        r'python-requests',
        r'libwww-perl',
        r'go-http-client',
        r'^apache-httpclient',
        r'^curl',
    ]

    if includeProgrammatic:
        regexs += programmatic_regexs

    regex = re.compile(
        '(' + '|'.join(regexs) + ')', re.IGNORECASE)

    crawler_user_agents_regexs = get_crawler_user_agents_regexs()
    for p in crawler_user_agents_regexs:
        if not includeProgrammatic and p in programmatic_regexs:
            continue
        if regex.search(p):  # or p in ['[ww]get', 'livelap[bb]ot']:
            continue
        p = p.replace(r'-', r'\-') \
            .replace(r'.', r'\.') \
            .replace(r'\/', r'') \
            .replace(r'(^| )', r'') \
            .replace(r'^', r'') \
            .replace(r' ', r'\s') \
            .replace(r'!', r'\!') \
            .replace(r'www.', r'')\
            .replace(r'www\\.', r'')
        p = re.sub(r'\.com.*', r'', p)
        p = re.sub(r'\\+$', r'', p)
        regexs.append(p)

    return '(' + '|'.join(regexs) + ')'


if __name__ == '__main__':
    print('Browser - Bots:')
    print(get_user_agent_regex())
    print('Programmatic + Browser - Bots:')
    print(get_user_agent_regex(includeProgrammatic=False))
