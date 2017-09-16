import htetl.extract.sites as sites


def test_extract_urls():
    data = [
        ('some text http://www.google.com',
         ['http://www.google.com']),
        ('foo https://somewhere.org/?foo=bar',
         ['https://somewhere.org/?foo=bar']),
        ('http://elsewhere.net/?bar=foo https://place.net/',
         ['http://elsewhere.net/?bar=foo', 'https://place.net/'])
    ]
    for text, expected in data:
        yield check_extract_urls, text, expected


def check_extract_urls(text, expected):
    result = sites.extract_urls(text)
    assert result == expected


def test_extract_sites():
    data = [
        ('some text http://www.google.com',
         ['www.google.com']),
        ('foo https://somewhere.org/?foo=bar',
         ['somewhere.org']),
        ('http://elsewhere.net/?bar=foo https://place.net/',
         ['elsewhere.net', 'place.net']),
        ('http://elsewhere.net/somewhere/here?bar=foo https://place.net/',
         ['elsewhere.net', 'place.net']),
    ]
    for text, expected in data:
        yield check_extract_sites, text, expected


def check_extract_sites(text, expected):
    result = sites.extract_sites(text)
    assert result == expected
