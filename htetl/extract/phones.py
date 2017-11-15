import re
import pandas as pd


DIGIT='(?:\d|one|two|three|four|five|six|seven|eight|nine|zero)\s*'
SEPARATOR = '(\s|-|\.|dash|dot)+'
NO_MATCH_SEPARATOR = '(?:-|\.|dash|dot)?'
AREA_CODE = '\(?(?:(?:%s){3})\)?' % DIGIT
FIRST_THREE = '\(?(?:(?:%s){3})\)?' % DIGIT
LAST_FOUR = '\(?(?:(?:%s){4})\)?' % DIGIT

# Regex variables for matching
NUMBER_REGEX = re.compile(''.join([
    '(',
    AREA_CODE, '\s*', NO_MATCH_SEPARATOR, '\s*', FIRST_THREE, '\s*',
    NO_MATCH_SEPARATOR, '\s*', LAST_FOUR,
    ')']),
    re.IGNORECASE)

SEPARATOR_REGEX = re.compile(SEPARATOR)

WORD_TO_DIGIT = list(enumerate([
    'zero', 'one', 'two', 'three', 'four',
    'five', 'six', 'seven', 'eight', 'nine'
]))


def normalize(text):
    '''gets rid of non numbers and puts everything in number format'''
    lower_text = text.lower()
    for d, w in WORD_TO_DIGIT:
        lower_text = lower_text.replace(w, str(d))
    return SEPARATOR_REGEX.sub('', lower_text)


def extract_phone(df):
    '''
        Does parsing logic
    '''
    return pd.DataFrame(
        [
            (page_id, normalize(match))
            for page_id, content in df[["id", "content"]].values
            for match in NUMBER_REGEX.findall(content)
        ],
        columns=["pageid", "phone"]
    ).drop_duplicates()
