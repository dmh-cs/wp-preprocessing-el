import json
import pydash as _
from unittest.mock import Mock

import process_pages as pp

def test_process_page():
  with open('test/fixtures/parade_page.json') as f:
    parade_page = json.load(f)
  with open('test/fixtures/parade_page_contexts.json') as f:
    parade_page_contexts = json.load(f)
  processed_page = pp.process_page(parade_page)
  assert _.predicates.is_equal(processed_page['document_info']['title'],
                               parade_page['title'])
  assert _.predicates.is_equal(processed_page['document_info']['text'],
                               parade_page['plaintext'])
  assert _.predicates.is_equal(processed_page['document_info']['categories'],
                               parade_page['categories'])
  assert _.predicates.is_equal(processed_page['link_contexts'],
                               parade_page_contexts)
  assert _.predicates.is_equal(processed_page['entity_counts'],
                               _.objects.map_values(parade_page_contexts, len))

def test_process_seed_pages():
  with open('test/fixtures/made.json') as f:
    made_up_page = json.load(f)
  with open('test/fixtures/koning.json') as f:
    koning_page = json.load(f)
  with open('test/fixtures/tweede.json') as f:
    tweede_page = json.load(f)
  with open('test/fixtures/parade_small_page.json') as f:
    parade_small_page = json.load(f)
  pages = {"Made up page": made_up_page,
           "Koning Edward VII-standbeeld": koning_page,
           "Tweede Vryheidsoorlog": tweede_page,
           "Parade": parade_small_page}
  pages_db = Mock()
  pages_db.find_one = lambda query: pages[query['_id']]
  depth_0 = pp.process_seed_pages(pages_db, [parade_small_page], depth=0)
  depth_1 = pp.process_seed_pages(pages_db, [parade_small_page], depth=1)
  depth_2 = pp.process_seed_pages(pages_db, [parade_small_page], depth=2)
  assert _.predicates.is_equal(set([page['document_info']['title'] for page in depth_0]),
                               {'Parade'})
  assert _.predicates.is_equal(set([page['document_info']['title'] for page in depth_1]),
                               {'Parade',
                                'Tweede Vryheidsoorlog',
                                'Koning Edward VII-standbeeld'})
  assert _.predicates.is_equal(set([page['document_info']['title'] for page in depth_2]),
                               {'Parade',
                                'Tweede Vryheidsoorlog',
                                'Koning Edward VII-standbeeld',
                                'Made up page'})
