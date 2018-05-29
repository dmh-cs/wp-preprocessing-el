from pymongo import MongoClient
import pymysql.cursors
from functools import reduce
import pydash as _
from dotenv import load_dotenv
import os
from pathlib import Path

from db import insert_wp_page, insert_category_associations, insert_link_contexts

def fetch_disambiguation_pages(pages_db):
  return pages_db.find({"title": {"$regex": "(disambiguation)"}})

def is_valid_page(page):
  flags = ['.jpg', '.svg', '.png', '.gif', '.jpeg', '.bmp', '.tiff', '(disambiguation)']
  if page and page.get('title'):
    return not any([_.strings.has_substr(page['title'].lower(), flag) for flag in flags])
  else:
    return False

def process_seed_pages(pages_db, seed_pages, depth=1):
  if depth != 1: raise NotImplementedError('Depth other than 1 not implemented yet')
  visited_page_titles = [page['title'] for page in seed_pages]
  processed_pages = [process_page(page) for page in seed_pages if is_valid_page(page)]
  link_names = sum([list(processed_page['link_contexts'].keys()) for processed_page in processed_pages],
                   [])
  pages_referenced = list(link_names)
  for page_title in _.arrays.difference(pages_referenced, visited_page_titles):
    page = pages_db.find_one({'_id': page_title})
    if is_valid_page(page):
      processed_pages.append(process_page(page))
  return processed_pages

def get_mention_offset(page_text, sentence_text, mention):
  return page_text.index(sentence_text) + sentence_text.index(mention)

def sentence_to_link_contexts(page, sentence):
  page_title = page['title']
  contexts = {}
  if sentence.get('links'):
    for link in sentence['links']:
      if link.get('page'):
        contexts[link['page']] = {'text': link['text'],
                                  'sentence': sentence['text'],
                                  'offset': get_mention_offset(page['plaintext'], sentence['text'], link['text']),
                                  'page_title': page_title}
  return contexts

def sentence_to_link_contexts_reducer(page, contexts_acc, sentence):
  contexts = sentence_to_link_contexts(page, sentence)
  if not _.predicates.is_empty(contexts):
    concat = lambda dest, src: dest + [src] if dest else [src]
    _.objects.merge_with(contexts_acc, contexts, iteratee=concat)
  return contexts_acc

def get_link_contexts(page):
  sections = page['sections']
  sentences = sum([section['sentences'] for section in sections], [])
  return reduce(_.functions.curry(sentence_to_link_contexts_reducer)(page), sentences, {})

def process_page(page):
  document_info = {'id': page['pageID'],
                   'title': page['title'],
                   'text': page['plaintext'],
                   'categories': page['categories']}
  link_contexts = get_link_contexts(page)
  entity_counts = _.objects.map_values(link_contexts, len)
  return {'document_info': document_info,
          'link_contexts': link_contexts,
          'entity_counts': entity_counts}

def merge_mentions(processed_pages):
  concat = lambda dest, src: dest + src if dest else src
  link_contexts = reduce(lambda acc, val: _.objects.merge_with(acc, val, iteratee=concat),
                         [processed_page['link_contexts'] for processed_page in processed_pages],
                         {})
  entity_counts = reduce(lambda acc, val: _.objects.merge_with(acc, val, iteratee=concat),
                          [processed_page['entity_counts'] for processed_page in processed_pages],
                          {})
  return _.objects.map_values(link_contexts,
                              lambda val, key: {'link_contexts': val,
                                                'entity_counts': entity_counts[key]})

def main():
  load_dotenv(dotenv_path=Path('../db') / '.env')
  DATABASE_NAME = os.getenv("DBNAME")
  DATABASE_USER = os.getenv("DBUSER")
  DATABASE_PASSWORD = os.getenv("DBPASS")
  DATABASE_HOST = os.getenv("DBHOST")

  client = MongoClient()
  dbname = 'afwiki'
  db = client[dbname]
  pages_db = db['pages']
  num_seed_pages = 10
  print('Fetching WP pages')
  initial_pages_to_fetch = list(pages_db.aggregate([{'$sample': {'size': num_seed_pages}}]))
  processed_pages = process_seed_pages(pages_db, initial_pages_to_fetch)
  print('Processing WP pages')

  connection = pymysql.connect(host=DATABASE_HOST,
                               user=DATABASE_USER,
                               password=DATABASE_PASSWORD,
                               db=DATABASE_NAME,
                               charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor)
  try:
    with connection.cursor() as cursor:
      print('Inserting processed pages')
      source = 'wikipedia'
      for processed_page in processed_pages:
        insert_wp_page(cursor, processed_page, source)
        connection.commit()
        insert_category_associations(cursor, processed_page, source)
        connection.commit()
        insert_link_contexts(cursor, processed_page, source)
        connection.commit()
  finally:
    connection.close()


if __name__ == "__main__": main()
