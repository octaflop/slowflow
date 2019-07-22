# DB connection boilerplate

import psycopg2

connection = psycopg2.connect(
    host="127.0.0.1",
    database="slowflow",
    user="slowflow",
    password="powers",
    port=5433
)
connection.autocommit = True

CREATE_TABLE = """
DROP TABLE IF EXISTS staging_beers;

CREATE UNLOGGED TABLE staging_beers (
    id                  INTEGER,
    name                TEXT,
    tagline             TEXT,
    first_brewed        DATE,
    description         TEXT,
    image_url           TEXT,
    abv                 DECIMAL,
    ibu                 DECIMAL,
    target_fg           DECIMAL,
    target_ob           DECIMAL,
    ebc                 DECIMAL,
    srm                 DECIMAL,
    ph                  DECIMAL,
    attenuation_level   DECIMAL,
    brewers_tips        TEXT,
    contributed_by      TEXT,
    volume              INTEGER
);
"""

# DB Creation fx
def create_staging_table(cursor) -> None:
    cursor.execute(CREATE_TABLE)


# parsers
import datetime

def parse_first_brewed(text: str) -> datetime.date:
    """Return a datetime object of text, a string
    >>> parse_first_brewed('09/2007')
    datetime.time(2007, 9, 1)
    >>> parse_first_brewed('2006')
    datetime.date(2006, 1, 1)
    """
    parts = text.split('/')
    if len(parts) == 2:
        return datetime.date(int(parts[1], int(parts[0])), 1)
    elif len(parts) == 1:
        return datetime.date(int(parts[0]), 1, 1)
    else:
        assert False, 'Unknown date format'

# data fetchers
from typing import Iterator, Dict, Any
from urllib.parse import urlencode
import requests

def iter_beers_from_api(page_size: int = 5) -> Iterator[Dict[str, Any]]:
    session = requests.Session()
    page = 1
    url = 'https://api.punkapi.com/v2/beers?'

    while True:
        response = session.get(url + urlencode({
            'page': page,
            'per_page': page_size
        }))
        response.raise_for_status()

        data = response.json()
        if not data:
            break
        
        yield from data

        page += 1

# metrics
import time
from functools import wraps
from memory_profiler import memory_usage

def profile(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        fn_kwargs_str = ', '.join(f'{k}={v}' for k, v in kwargs.items())
        print(f'\n{fn.__name__}({fn_kwargs_str})')

        # measure time
        t = time.perf_counter()
        retval = fn(*args, **kwargs)
        elapsed = time.perf_counter() - t
        print(f'Time {elapsed:0.4}')

        # measure memory
        mem, retval = memory_usage(
            (fn, args, kwargs), retval=True, timeout=200, interval=1e-7)
        print(f'Memory {max(mem) - min(mem)}')
        return retval
    
    return inner

# Stuff to profile

## Insert rows one-by-one
@profile
def insert_one_by_one(connection, beers: Iterator[Dict[str, Any]]) -> None:
    with connection.cursor() as cursor:
        create_staging_table(cursor)
        for beer in beers:
            cursor.execute("""
                INSERT INTO staging_beers VALUES (
                    %(id)s,
                    %(name)s,
                    %(tagline)s,
                    %(first_brewed)s,
                    %(description)s,
                    %(image_url)s,
                    %(abv)s,
                    %(ibu)s,
                    %(target_fg)s,
                    %(target_og)s,
                    %(ebc)s,
                    %(srm)s,
                    %(ph)s,
                    %(attenuation_level)s,
                    %(brewers_tips)s,
                    %(contributed_by)s,
                    %(volume)s
                );
            """, {
                **beer,
                'first_brewed': parse_first_brewed(beer['first_brewed']),
                'volume': beer['volume']['value'],
            })

@profile
def insert_executemany(connection, beers: Iterator[Dict[str, Any]]) -> None:
    with connection.cursor as cursor:
        create_staging_table(cursor)
        all_beers = [{
            **beer,
            'first_brewed': parse_first_brewed(beer['first_brewed']),
            'volume': beer['volume']['value'],
        } for beer in beers]

        cursor.executemany("""
            INSERT INTO staging_beers VALUES (
                %(id)s,
                %(name)s,
                %(tagline)s,
                %(first_brewed)s,
                %(description)s,
                %(image_url)s,
                %(abv)s,
                %(ibu)s,
                %(target_fg)s,
                %(target_og)s,
                %(ebc)s,
                %(srm)s,
                %(ph)s,
                %(attenuation_level)s,
                %(brewers_tips)s,
                %(contributed_by)s,
                %(volume)s
            );
        """, all_beers)

@profile
def insert_executemany_iterator(connection, beers: Iterator[Dict[str, Any]]) -> None:
    with connection.cursor() as cursor:
        create_staging_table(cursor)
        cursor.executemany("""
            INSERT INTO staging_beers VALUES (
                %(id)s,
                %(name)s,
                %(tagline)s,
                %(first_brewed)s,
                %(description)s,
                %(image_url)s,
                %(abv)s,
                %(ibu)s,
                %(target_fg)s,
                %(target_og)s,
                %(ebc)s,
                %(srm)s,
                %(ph)s,
                %(attenuation_level)s,
                %(brewers_tips)s,
                %(contributed_by)s,
                %(volume)s
            );
        """, ({
            **beer,
            'first_brewed': parse_first_brewed(beer['first_brewed']),
            'volume': beer['volume']['value']
        } for beer in beers))

@profile
def insert_execute_batch(connection, beers: Iterator[Dict[str, Any]]) -> None:
    with connection.cursor() as cursor:
        create_staging_table(cursor)

        all_beers = [{
            **beer,
            'first_brewed': parse_first_brewed(beer['first_brewed']),
            'volume': beer['volume']['value'],
        } for beer in beers]

        psycopg2.extras.execute_batch(cursor, """
            INSERT INTO staging_beers VALUES (
                %(id)s,
                %(name)s,
                %(tagline)s,
                %(first_brewed)s,
                %(description)s,
                %(image_url)s,
                %(abv)s,
                %(ibu)s,
                %(target_fg)s,
                %(target_og)s,
                %(ebc)s,
                %(srm)s,
                %(ph)s,
                %(attenuation_level)s,
                %(brewers_tips)s,
                %(contributed_by)s,
                %(volume)s
            );
        """, all_beers)

def main() -> None:
    beers = list(iter_beers_from_api) * 100
    insert_one_by_one(connection, beers)


if __name__ == '__main__':
    print('Creating table')
    main()
