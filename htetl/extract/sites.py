import rfc3987

URL_REGEX=rfc3987.get_compiled_pattern('%(URI)s')

def extract_urls(text):
    '''Extract urls from given text

    :text: (str) Text to extract urls from
    :returns: List of urls extracted

    '''

    return URL_REGEX.findall(text)


def extract_sites(text):
    '''Extract sites from urls in given text

    :text: (str) Text to extract sites from
    :returns: List of sites extracted
    '''
    urls = URL_REGEX.findall(text)
    sites = set(
        rfc3987.parse(u, rule='URI')['authority']
        for u in urls
    )

    return list(sites)
