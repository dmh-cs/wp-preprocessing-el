import json
import pydash.predicates as _

import create_entity_to_context as cc

def test_process_page():
  with open('test/fixtures/parade_page.json') as f:
    parade_page = json.load(f)
  with open('test/fixtures/parade_page_contexts.json') as f:
    parade_page_contexts = json.load(f)
  processed_page = cc.process_page(parade_page)
  assert _.is_equal(processed_page['document_info']['title'], parade_page['title'])
  assert _.is_equal(processed_page['document_info']['desc'], parade_page['plaintext'][:100])
  assert _.is_equal(processed_page['document_info']['categories'], parade_page['categories'])
  assert _.is_equal(processed_page['link_contexts'], parade_page_contexts)
