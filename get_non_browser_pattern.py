import urllib.request
import json
import re


def fetch_json(url):
    with urllib.request.urlopen(url) as url_opened:
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
    return "(bot|python|wget|curl|perl|java|crawler|uniprot|monitor|searchhelper|winhttp|ruby|scraper|skyline|\\-|nucpredthe\\sknowledge\\sai|slurp|httpunit|nutch|phpcrawl|biglotron|teoma|convera|gigablast|ia_archiver|webmon\\s|httrack|grub\\.org|netresearchserver|speedy|fluffy|findlink|panscient|yanga|yandeximages|cyberpatrol|baiduspider|postrank|page2rss|linkdex|ezooms|heritrix|findthatfile|europarchive\\.org|mappydata|eright|apercite|aboundex|summify|ec2linkfinder|facebookexternalhit|yeti|retrevopageanalyzer|sogou|wotbox|ichiro|drupact|openindexspider|gnam\\sgnam\\sspider|coccoc|integromedb|siteexplorer\\.info|proximic|changedetection|wesee:search|360spider|cc\\smetadata\\sscaper|g00g1e\\.net|binlar|admantx|megaindex|ltx71|bubing|qwantify|lipperhey|y\\!j|addthis|screaming\\sfrog\\sseo\\sspider|metauri|scrapy|capsulechecker|collection@infegy|deusu|sonic|sysomos|trove|deadlinkchecker|embedly|iskanie|skypeuripreview|whatsapp|electricmonk|bingpreview|yahoo\\slink\\spreview|daum|xenu\\slink\\ssleuth|pingdom|appinsights|phantomjs|jetslide|newsharecounts|barkrowler|tineye|linkarchiver|yak|digg\\sdeeper|dcrawl|snacktory|ning|okhttp|nuzzel|omgili|pocketparser|yisouspider|toutiaospider|muckrack|jamie's\\sspider|ahc|netcraftsurveyagent|jetty|upflow|thinklab|traackr|twurly|mastodon|http_get|brandverity|check_http|ezid|lcc\\s|buck|genieo|meltwaternews|moreover|newspaper|scoutjet|sentry|seoscanners|hatena|google\\sweb\\spreview|adscanner|netvibes|btwebclient|disqus|feedly|fetch|fever|flamingo_searchengine|flipboardproxy|g2\\sweb\\sservices|vkshare|siteimprove|dareboost|miniflux|feedspot|seokicks|tracemyfile|zgrab|datafeedwatch|zabbix|axios|amazon\\scloudfront|pulsepoint|wordupinfosearch|webdatastats|httpurlconnection|outbrain|w3c_validator|validator\\\\.nu|feedvalidator|w3c_css_validator|w3c_unicorn|blackboard|bazqux|twingly|rivva|dataprovider|grouphigh|theoldreader|anyevent|nmap\\sscripting\\sengine|2ip\\.ru|clickagy|google\\sfavicon|hubspot)"
