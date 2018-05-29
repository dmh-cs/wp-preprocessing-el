import json
import pydash as _

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
