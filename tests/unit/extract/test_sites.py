import random
import string
import time

import htetl.extract.sites as sites
import pandas as pd


def test_extract_urls():
    data = [
        ('some text http://www.google.com',
         ['http://www.google.com']),
        ('foo https://somewhere.org/?foo=bar',
         ['https://somewhere.org/?foo=bar']),
        ('http://elsewhere.net/?bar=foo https://place.net/',
         ['http://elsewhere.net/?bar=foo', 'https://place.net/']),
        ('<http://subdomain.elsewhere.net/?bar=foo https://place.net/',
         ['http://subdomain.elsewhere.net/?bar=foo', 'https://place.net/']),
        ('some text google.com',
         ['google.com']),
    ]
    for text, expected in data:
        yield check_extract_urls, text, expected


def check_extract_urls(text, expected):
    result = sites.extract_urls(text)
    assert result == expected


def test_extract_sites():
    data = [
        ('some text http://www.google.com',
         ['google.com']),
        ('foo https://somewhere.org/?foo=bar',
         ['somewhere.org']),
        ('http://elsewhere.net/?bar=foo https://place.net/',
         ['elsewhere.net', 'place.net']),
        ('http://elsewhere.net/somewhere/here?bar=foo https://place.net/',
         ['elsewhere.net', 'place.net']),
        ('http://subdomain.elsewhere.net/?bar=foo https://place.net/',
         ['elsewhere.net', 'place.net']),
        ('http://sub.subdomain.elsewhere.net/?bar=foo https://place.net/',
         ['elsewhere.net', 'place.net']),
        ('site.org/some/page.html picture.jpg',
         ['site.org']),
    ]
    for text, expected in data:
        yield check_extract_sites, text, expected


def check_extract_sites(text, expected):
    result = sites.extract_sites(text)
    assert result == expected


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
    result = [sites.extract_sites(c) for c in df['content'].values]
    end = time.time()
    duration = end - start

    assert len(result) == post_count

    # Threshold determined with assumption of 250k posts needing to be
    # analyzed in <6 hours (goal is the 250k posts should be completely
    # analyzed in <24 hours), with average of 25k characters in each post
    # (determined in data extract on 2017-11-13).
    assert post_count * 1.0 / duration >= (250000.0 / (6 * 60 * 60))


test_extract_sites_performance.slow = 1
