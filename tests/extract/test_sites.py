import htetl.extract.sites as sites

def test_extract_urls():
    expected = 'http://www.google.com'
    result = sites.extract_urls(expected)

    assert result == [expected]
