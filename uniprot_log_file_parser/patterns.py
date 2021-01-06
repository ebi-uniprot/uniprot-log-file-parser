import json
from urllib import parse, request
import re

CRAWLER_USER_AGENTS = 'https://raw.githubusercontent.com/monperrus/crawler-user-agents/master/crawler-user-agents.json'


def combine_regexps(regexps):
    return f'({"|".join(regexps)})'


def compile_regexp(regexp):
    return re.compile(regexp, re.IGNORECASE)


# This is a cached result of generate_and_print_bot_regexp to prevent having to regenerate for each job in the job array.
# TODO: save in a cache location on NFS with https://github.com/ActiveState/appdirs
BOT_RE = compile_regexp(r"(bot|crawler|monitor|the\sknowledge\sai|searchhelper|winhttp|scraper|skyline|nucpred|the\sknowledge\sai|arachni|siteuptime|microsoft\soffice|riddler|validator|feedfetcher\-google|mediapartners\-google|apis\-google|slurp|[ww]get|httpunit|nutch|phpcrawl|biglotron|teoma|convera|gigablast|ia_archiver|webmon\s|httrack|grub\.org|netresearchserver|speedy|fluffy|findlink|panscient|ips\-agent|yanga|yandeximages|yandexmetrika|yandexturbo|yandeximageresizer|yandexvideo|yandexadnet|yandexblogs|yandexcalendar|yandexdirect|yandexfavicons|yadirectfetcher|yandexfordomain|yandexmarket|yandexmedia|yandexnews|yandexontodb|yandexpagechecker|yandexpartner|yandexrca|yandexsearchshop|yandexsitelinks|yandextracker|yandexvertis|yandexverticals|yandexwebmaster|cyberpatrol|baiduspider|postrank|page2rss|linkdex|ezooms|heritrix|findthatfile|europarchive\.org|mappydata|eright|apercite|aboundex|summify|ec2linkfinder|facebookexternalhit|yeti|retrevopageanalyzer|lb\-spider|sogou|wotbox|ichiro|drupact|openindexspider|gnam\sgnam\sspider|coccoc|integromedb|siteexplorer\.info|proximic|changedetection|wesee:search|360spider|cc\smetadata\sscaper|g00g1e\.net|binlar|a6\-indexer|admantx|megaindex|ltx71|bubing|qwantify|lipperhey|y\!j|addthis|screaming\sfrog\sseo\sspider|metauri|scrapy|livelap[bb]ot|capsulechecker|collection@infegy|deusu|sonic|sysomos|trove|deadlinkchecker|slack\-imgproxy|embedly|iskanie|skypeuripreview|google\-adwords\-instant|whatsapp|electricmonk|bingpreview|yahoo\slink\spreview|daum|xenu\slink\ssleuth|pcore\-http|pingdom|appinsights|phantomjs|jetslide|newsharecounts|bark[rr]owler|tineye|linkarchiver|yak|digg\sdeeper|dcrawl|snacktory|ning|okhttp|nuzzel|omgili|pocketparser|yisouspider|um\-ln|toutiaospider|muckrack|jamie's\sspider|ahc|netcraftsurveyagent|appengine\-google|jetty|upflow|thinklab|traackr|twurly|mastodon|http_get|brandverity|check_http|ezid|lcc\s|buck|genieo|meltwaternews|moreover|newspaper|scoutjet|sentry|seoscanners|hatena|google\sweb\spreview|adscanner|netvibes|baidu\-yunguance|btwebclient|disqus|feedly|fetch|fever|flamingo_searchengine|flipboardproxy|g2\sweb\sservices|landau\-media\-spider|vkshare|siteimprove|dareboost|miniflux|feedspot|seokicks|tracemyfile|zgrab|pr\-cy\.ru|datafeedwatch|zabbix|google\-xrawler|axios|amazon\scloudfront|pulsepoint|cloudflare\-alwaysonline|google\-structured\-data\-testing\-tool|wordupinfosearch|webdatastats|httpurlconnection|outbrain|w3c\-checklink|w3c\-mobileok|w3c_i18n\-checker|w3c_unicorn|google\-physicalweb|blackboard|bazqux|twingly|rivva|dataprovider|grouphigh|theoldreader|anyevent|nmap\sscripting\sengine|2ip\.ru|clickagy|google\sfavicon|hubspot|chrome\-lighthouse|headlesschrome|checkmarknetwork|uptime|mixnodecache|fedoraplanet|friendica|nextcloud|tiny\stiny\srss|bytespider|datanyze|google\-site\-verification|trendsmapresolver|tweetedtimes|gwene|simplepie|searchatlas|superfeedr|domains\sproject|pandalytics|pagepeeker|vigil|seewithkids|blogtraffic\d\\.\d+\sfeed\-fetcher|cincraw|freshrss|php\-curl\-class)")


UNKNOWN_RE = compile_regexp(combine_regexps([
    r'^\-$',
]))

PROGRAMMATIC_RE = compile_regexp(combine_regexps([
    r'wget',
    r'[wW]get',
    r'python',
    r'perl',
    r'go\-http\-client',
    r'apache\-httpclient',
    r'^curl',
    r'\^curl',
]))


def get_crawler_user_agents_regexps():
    with request.urlopen(CRAWLER_USER_AGENTS) as url_opened:
        crawlers = json.loads(url_opened.read().decode())
        return [crawler['pattern'].lower() for crawler in crawlers]


def clean_regexp(p):
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
    return p


def generate_and_print_bot_regexp():
    regexps = [
        r'bot',
        r'crawler',
        r'monitor',
        r'the\sknowledge\sai',
        r'searchhelper',
        r'winhttp',
        r'scraper',
        r'skyline',
        r'nucpred',
        r'the\sknowledge\sai',
        r'arachni',
        r'siteuptime',
        r'microsoft\soffice',
        r'riddler',
        r'validator',
    ]

    regexp = compile_regexp(combine_regexps(regexps))
    crawler_user_agents_regexps = get_crawler_user_agents_regexps()
    for p in crawler_user_agents_regexps:
        if not (regexp.search(p) or PROGRAMMATIC_RE.search(p) or UNKNOWN_RE.search(p)):
            regexps.append(clean_regexp(p))
    print('Bots regular expression:')
    print(combine_regexps(regexps))


if __name__ == '__main__':
    generate_and_print_bot_regexp()
