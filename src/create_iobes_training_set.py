import pydash as _
import os
import pymysql.cursors
from dotenv import load_dotenv
from pathlib import Path
from progressbar import progressbar

from db import get_page_mentions, get_pages_having_mentions, get_page_titles
from iobes import get_page_iobes, write_page_iobes


def main():
  load_dotenv(dotenv_path=Path('../db') / '.env')
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
        mentions = get_page_mentions(cursor, page_id)
        mention_link_titles = get_page_titles(cursor, _.collections.pluck(mentions, 'page_id'))
        page_iobes = get_page_iobes(page, mentions, mention_link_titles)
        write_page_iobes(page, page_iobes)
  finally:
    connection.close()


if __name__ == "__main__": main()
