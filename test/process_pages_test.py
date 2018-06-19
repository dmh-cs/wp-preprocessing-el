import json
import pydash as _
from unittest.mock import Mock

import process_pages as pp

def test_sentence_to_link_contexts():
  redirects_lookup = {'some text': 'Some Words'}
  page = {'_id': 'My page', 'pageID': 0, 'title': 'My page', 'categories': [], 'plaintext': 'some text',
          'sections': [{'sentences': [{'text': 'some text', 'links': [{'page': 'some text'}]}]}]}
  sentence = {'text': 'some text', 'links': [{'page': 'some text'}]}
  link_contexts = {'Some Words':
                   [{'text': 'some text', 'offset': 0, 'page_title': 'My page', 'sentence': 'some text'}]}
  assert link_contexts == pp.sentence_to_link_contexts(redirects_lookup, page, sentence)

def test__mention_overlaps():
  mentions = [{'text': 'some other text', 'offset': 0, 'page_title': 'Other'},
              {'text': 'my', 'offset': 20, 'page_title': 'My page'}]
  mention = {'text': 'other', 'offset': 5, 'page_title': 'Other'}
  assert pp._mention_overlaps(mentions, mention)

def test_process_page():
  with open('test/fixtures/parade_page.json') as f:
    parade_page = json.load(f)
  with open('test/fixtures/parade_page_contexts.json') as f:
    parade_page_contexts = json.load(f)
  redirects_lookup = {}
  processed_page = pp.process_page(redirects_lookup, parade_page)
  assert processed_page['document_info']['title'] == parade_page['title']
  assert processed_page['document_info']['text'] == parade_page['plaintext']
  assert processed_page['document_info']['categories'] == parade_page['categories']
  assert processed_page['link_contexts'] == parade_page_contexts
  assert processed_page['entity_counts'] == _.map_values(parade_page_contexts, len)

def test_process_page_with_implicit_links():
  page = {'_id': 'My page', 'pageID': 0, 'title': 'My page', 'categories': [], 'plaintext': 'some text',
          'sections': [{'sentences': [{'text': 'some text', 'links': [{'page': 'some'}]}]}]}
  redirects_lookup = {}
  processed_page = pp.process_page(redirects_lookup, page)
  assert processed_page['document_info']['title'] == 'My page'
  assert processed_page['document_info']['text'] == 'some text'
  assert processed_page['document_info']['categories'] == []
  assert processed_page['link_contexts'] == {'My page': [],
                                             'Some': [{'text': 'some',
                                                       'sentence': 'some text',
                                                       'offset': 0,
                                                       'page_title': 'My page'}]}
  assert processed_page['entity_counts'] == {'My page': 0,
                                             'Some': 1}

def test_process_page_with_overlapping_mentions():
  page = {'_id': 'Other', 'pageID': 0, 'categories': [], 'title': 'Other',
          'plaintext': 'some Other text and my stuff',
          'sections': [{'sentences': [{'text': 'some Other text and my stuff',
                                       'links': [{'page': 'Other', 'text': 'some Other text'},
                                                 {'page': 'My page', 'text': 'my'}]}]}]}
  redirects_lookup = {}
  processed_page = pp.process_page(redirects_lookup, page)
  assert processed_page['document_info']['title'] == 'Other'
  assert processed_page['document_info']['text'] == 'some Other text and my stuff'
  assert processed_page['link_contexts'] == {'Other': [{'text': 'some Other text',
                                                        'sentence': 'some Other text and my stuff',
                                                        'offset': 0,
                                                        'page_title': 'Other'}],
                                             'My page': [{'text': 'my',
                                                          'sentence': 'some Other text and my stuff',
                                                          'offset': 20,
                                                          'page_title': 'Other'}]}
  assert processed_page['entity_counts'] == {'Other': 1,
                                             'My page': 1}

def test_process_page_with_redirects():
  with open('test/fixtures/parade_page.json') as f:
    parade_page = json.load(f)
  with open('test/fixtures/parade_page_contexts.json') as f:
    parade_page_contexts = json.load(f)
  redirects_lookup = {"Fort de Goede Hoop": "Kaapstad"}
  processed_page = pp.process_page(redirects_lookup, parade_page)
  assert processed_page['document_info']['title'] == parade_page['title']
  assert processed_page['document_info']['text'] == parade_page['plaintext']
  assert processed_page['document_info']['categories'] == parade_page['categories']
  parade_page_contexts["Kaapstad"].insert(1, parade_page_contexts.pop("Fort de Goede Hoop")[0])
  assert processed_page['link_contexts'] == parade_page_contexts
  assert processed_page['entity_counts'] == _.map_values(parade_page_contexts, len)

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
  redirects_lookup = {}
  pages_db = Mock()
  pages_db.find_one = lambda query: pages[query['_id']]
  depth_0 = pp.process_seed_pages(pages_db, redirects_lookup, [parade_small_page], depth=0)
  depth_1 = pp.process_seed_pages(pages_db, redirects_lookup, [parade_small_page], depth=1)
  depth_2 = pp.process_seed_pages(pages_db, redirects_lookup, [parade_small_page], depth=2)
  assert _.is_equal(set([page['document_info']['title'] for page in depth_0]),
                    {'Parade'})
  assert _.is_equal(set([page['document_info']['title'] for page in depth_1]),
                    {'Parade',
                     'Tweede Vryheidsoorlog',
                     'Koning Edward VII-standbeeld'})
  assert _.is_equal(set([page['document_info']['title'] for page in depth_2]),
                    {'Parade',
                     'Tweede Vryheidsoorlog',
                     'Koning Edward VII-standbeeld',
                     'Made up page'})
