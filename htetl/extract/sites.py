import re

# Old rfc3987 library based regex does not cover what is human-intepreted as
# a URL (e.g. www.google.com)
# Not sure if ftp and sftp are necessary
PROTOCOLS = r'((http|https|ftp|sftp)://)'

SUBDOMAIN = r'([a-z0-9]+\.)'
DOMAIN_NAME = r'([a-z0-9]+)'
DOMAIN = r'((\.[a-z]{2,3})?\.[a-z]{2,4})'
HOST = SUBDOMAIN + '?' + DOMAIN_NAME + DOMAIN

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
        PROTOCOLS, '?',
        SUBDOMAIN, '*',
        '(', DOMAIN_NAME, DOMAIN, PORT, '?)',
    ]),
    re.IGNORECASE
)

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
    sites = [m[3] for m in SITE_REGEX.findall(text)]

    return sites
