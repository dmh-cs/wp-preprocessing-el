from functools import reduce
import re
import pydash as _

def is_valid_page(page):
  flags = ['.jpg', '.svg', '.png', '.gif', '.jpeg', '.bmp', '.tiff', '(disambiguation)']
  if page and page.get('title'):
    return not any([_.has_substr(page['title'].lower(), flag) for flag in flags])
  else:
    return False

def is_valid_link(link):
  flags = ['.jpg', '.svg', '.png', '.gif', '.jpeg', '.bmp', '.tiff']
  if link and link.get('text') and link.get('page'):
    valid_text = not any([_.has_substr(link['text'].lower(), flag) for flag in flags])
    valid_page = not any([_.has_substr(link['page'].lower(), flag) for flag in flags])
    return valid_text and valid_page
  else:
    return False

def get_outlinks(processed_pages):
  link_names = sum([list(processed_page['link_contexts'].keys()) for processed_page in processed_pages],
                   [])
  return set(link_names)

def _process_pages(pages):
  return [process_page(page) for page in pages if is_valid_page(page)]

def _fetch_pages(pages_db, page_titles):
  return [pages_db.find_one({'_id': title}) for title in page_titles]

def process_seed_pages(pages_db, seed_pages, depth=1):
  processed_pages = _process_pages(seed_pages)
  latest_processed_pages = processed_pages
  visited_page_titles = set([processed_page['document_info']['title'] for processed_page in processed_pages])
  for layer in range(depth):
    pages_referenced = get_outlinks(latest_processed_pages)
    page_titles_to_fetch = pages_referenced - visited_page_titles
    pages_to_process = _fetch_pages(pages_db, page_titles_to_fetch)
    latest_processed_pages = _process_pages(pages_to_process)
    processed_pages += latest_processed_pages
    visited_page_titles = visited_page_titles.union(pages_referenced)
  return processed_pages

def get_mention_offset(page_text, sentence_text, mention):
  try:
    sentence_page_offset = page_text.index(sentence_text)
  except ValueError:
    raise ValueError('Sentence not found in page')
  try:
    mention_sentence_offset = sentence_text.index(mention)
  except ValueError:
    raise ValueError('Mention not found in sentence')
  return sentence_page_offset + mention_sentence_offset

def sentence_to_link_contexts(page, sentence):
  page_title = page['title']
  contexts = {}
  if sentence.get('links'):
    for link in sentence['links']:
      if link.get('page') and is_valid_link(link):
        try:
          mention_offset = get_mention_offset(page['plaintext'], sentence['text'], link['text'])
          contexts[link['page']] = {'text': link['text'],
                                    'sentence': sentence['text'],
                                    'offset': mention_offset,
                                    'page_title': page_title}
        except ValueError:
          continue
  return contexts

def sentence_to_link_contexts_reducer(page, contexts_acc, sentence):
  contexts = sentence_to_link_contexts(page, sentence)
  if not _.is_empty(contexts):
    concat = lambda dest, src: dest + [src] if dest else [src]
    _.merge_with(contexts_acc, contexts, iteratee=concat)
  return contexts_acc

def get_link_contexts(page):
  sections = page['sections']
  sentences = sum([section['sentences'] for section in sections], [])
  sentences_from_tables = sum([[table['data'] for table in section['tables'][0] if table.get('data')] for section in sections if section.get('tables')],
                              [])
  all_sentences = sentences + sentences_from_tables
  return reduce(_.curry(sentence_to_link_contexts_reducer)(page), all_sentences, {})

def _page_title_exact_match_heuristic(page, link_contexts):
  matches = re.finditer(page['title'], page['plaintext'])
  link_context = {page['title']: [{'text': page['title'],
                                   'offset': match.start(0),
                                   'page_title': page['title']} for match in matches]}
  concat = lambda dest, src: dest + src if dest else src
  if not _.is_empty(link_context[page['title']]):
    return _.merge_with(link_contexts, link_context, iteratee=concat)
  else:
    return link_contexts

def get_link_contexts_using_heuristics(page):
  link_contexts = get_link_contexts(page)
  return _page_title_exact_match_heuristic(page, link_contexts)

def process_page(page):
  document_info = {'source_id': page['pageID'],
                   'title': page['title'],
                   'text': page['plaintext'],
                   'categories': page['categories']}
  link_contexts = get_link_contexts_using_heuristics(page)
  entity_counts = _.map_values(link_contexts, len)
  return {'document_info': document_info,
          'link_contexts': link_contexts,
          'entity_counts': entity_counts}

def merge_mentions(processed_pages):
  concat = lambda dest, src: dest + src if dest else src
  link_contexts = reduce(lambda acc, val: _.merge_with(acc, val, iteratee=concat),
                         [processed_page['link_contexts'] for processed_page in processed_pages],
                         {})
  entity_counts = reduce(lambda acc, val: _.merge_with(acc, val, iteratee=concat),
                          [processed_page['entity_counts'] for processed_page in processed_pages],
                          {})
  return _.map_values(link_contexts,
                      lambda val, key: {'link_contexts': val,
                                        'entity_counts': entity_counts[key]})
