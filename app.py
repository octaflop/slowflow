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

def create_staging_table(cursor) -> None:
    cursor.execute(CREATE_TABLE)


def main() -> None:
    with connection.cursor() as cursor:
        create_staging_table(cursor)

if __name__ == '__main__':
    print('Creating table')
    main()
