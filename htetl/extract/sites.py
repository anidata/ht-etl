import re
from htetl.extract.tld import TLD_LIST

# Old rfc3987 library based regex does not cover what is human-intepreted as
# a URL (e.g. www.google.com)
# Not sure if ftp and sftp are necessary
PROTOCOLS = r'(?:(?:http|https|ftp|sftp)://)'


SUBDOMAIN = r'(?:[a-z0-9]+\.)'
DOMAIN_NAME = r'([a-z0-9]+)'
TLD_REGEX = '(?:' + '|'.join(reversed(TLD_LIST)) + ')'
HOST = SUBDOMAIN + '?' + DOMAIN_NAME + '\.' + TLD_REGEX

PORT = r'(:[0-9]{0,6})'
AUTHORITY = '(' + HOST + PORT + '?)'

URL_REGEX = re.compile(
    ''.join([
        '(',
        PROTOCOLS, '?',
        AUTHORITY, '/?',
        '(\S+)?',
        ')',
    ]),
    re.IGNORECASE
)

SITE_REGEX = re.compile(
    ''.join([
        r'\b',
        PROTOCOLS, '?',
        SUBDOMAIN, '*',
        '(', DOMAIN_NAME, '\.', TLD_REGEX, PORT, r'?)/?\b',
    ]),
    re.IGNORECASE
)

# Modified from
# https://daringfireball.net/2010/07/improved_regex_for_matching_urls
# SITE_REGEX = re.compile(
#     r'''(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'".,]))''',
#     re.IGNORECASE
# )

def extract_urls(text):
    '''Extract urls from given text

    :text: (str) Text to extract urls from
    :returns: List of urls extracted

    '''

    return [m[0] for m in URL_REGEX.findall(text)]


def extract_sites(text):
    '''Extract sites from urls in given text

    :text: (str) Text to extract sites from
    :returns: List of sites extracted
    '''
    sites = [m[0] for m in SITE_REGEX.findall(text)]
    return sites


def test_extract_sites_performance():
    post_count = 2500
    post_character_count = 25000
    example_site = 'site.org'

    choice_string = string.ascii_uppercase + string.ascii_lowercase + string.digits
    data = [
        (
            i,
            example_site + ' ' + ''.join(
                random.choice(choice_string)
                for _ in range(post_character_count)
            )
        )
        for i in range(post_count)
    ]

    df = pd.DataFrame(data, columns=["id", "content"])

    start = time.time()
    result_df = extract_sites(df)
    end = time.time()
    duration = end - start

    assert len(result_df) == post_count

    # Threshold determined with assumption of 250k posts needing to be
    # analyzed in <6 hours (goal is the 250k posts should be completely
    # analyzed in <24 hours), with average of 25k characters in each post
    # (determined in data extract on 2017-11-13).
    assert post_count * 1.0 / duration >= (250000.0 / (6 * 60 * 60))


test_extract_sites_performance.slow = 1
