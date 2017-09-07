import rfc3987

URL_REGEX=rfc3987.get_compiled_pattern('%(URI)s')

def extract_urls(text):
    '''Extract urls from given text

    :text: (str) Text to extract urls from
    :returns: List of urls extracted

    '''

    return URL_REGEX.findall(text)
