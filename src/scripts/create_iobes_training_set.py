import pydash as _
import os
import pymysql.cursors
from dotenv import load_dotenv
from pathlib import Path
from progressbar import progressbar

import sys
sys.path.append('./src')

from db import get_page_mentions_by_entity, get_pages_having_mentions, get_page_titles
from iobes import get_page_iobes, write_page_iobes


def main():
  load_dotenv(dotenv_path=Path('db') / '.env')
  DATABASE_NAME = os.getenv("DBNAME")
  DATABASE_USER = os.getenv("DBUSER")
  DATABASE_PASSWORD = os.getenv("DBPASS")
  DATABASE_HOST = os.getenv("DBHOST")
  connection = pymysql.connect(host=DATABASE_HOST,
                               user=DATABASE_USER,
                               password=DATABASE_PASSWORD,
                               db=DATABASE_NAME,
                               charset='utf8mb4',
                               use_unicode=True,
                               cursorclass=pymysql.cursors.DictCursor)
  try:
    with connection.cursor() as cursor:
      cursor.execute("SET NAMES utf8mb4;")
      cursor.execute("SET CHARACTER SET utf8mb4;")
      cursor.execute("SET character_set_connection=utf8mb4;")
      for page in progressbar(get_pages_having_mentions(cursor)):
        page_id = page['id']
        sorted_mentions = get_page_mentions_by_entity(cursor, page_id)
        mention_link_titles = _.collections.pluck(sorted_mentions, 'entity')
        page_iobes = get_page_iobes(page, sorted_mentions, mention_link_titles)
        write_page_iobes(page, page_iobes)
  finally:
    connection.close()


if __name__ == "__main__": main()
