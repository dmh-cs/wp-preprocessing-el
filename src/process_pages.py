from functools import reduce
import pydash as _
from toolz import pipe
from progressbar import progressbar

import utils as u
from data_cleaners import clean_page

def is_valid_page(page):
  '''Invalid pages are images or disambiguation pages that were not
flagged at the parser level. Also check that the page has more than 5 characters.'''
  flags = ['.jpg', '.svg', '.png', '.gif', '.jpeg', '.bmp', '.tiff', '(disambiguation)']
  has_content = page and (len(page['plaintext'].strip()) > 5)
  if page and has_content and 'title' in page:
    return not any([_.has_substr(page['title'].lower(), flag) for flag in flags])
  else:
    return False

def is_valid_link(link):
  '''Invalid links are to images. This works for both implicit and regular style links.'''
  flags = ['.jpg', '.svg', '.png', '.gif', '.jpeg', '.bmp', '.tiff']
  result = True
  if link and 'page' in link:
    result = result and (not any([_.has_substr(link['page'].lower(), flag) for flag in flags]))
  else:
    return False
  if link and 'text' in link:
    result = result and (not any([_.has_substr(link['text'].lower(), flag) for flag in flags]))
  return result

def get_outlinks(processed_pages):
  '''set of page names that these pages link to'''
  link_names = sum([list(processed_page['link_contexts'].keys()) for processed_page in processed_pages],
                   [])
  return set(link_names)

def _process_pages(redirects_lookup, pages, is_seed_page=False):
  return [process_page(redirects_lookup,
                       page,
                       is_seed_page=is_seed_page) for page in pages if is_valid_page(page)]

def _fetch_pages(pages_db, page_titles):
  return [pages_db.find_one({'_id': title}) for title in page_titles]

def process_seed_pages(pages_db, redirects_lookup, seed_pages, depth=1):
  '''Get the mentions in each of the seed pages as well as the pages
they link to. Set `depth` > 1 to also process the pages that those
pages link to'''
  processed_pages = _process_pages(redirects_lookup, seed_pages, is_seed_page=True)
  latest_processed_pages = processed_pages
  visited_page_titles = set([processed_page['document_info']['title'] for processed_page in processed_pages])
  for layer in range(depth):
    print("Getting referenced pages")
    pages_referenced = get_outlinks(latest_processed_pages)
    page_titles_to_fetch = pages_referenced - visited_page_titles
    batch_size = 1000
    print("Fetching and processing", len(page_titles_to_fetch), "pages in", batch_size, "batches")
    for batch_num, titles_batch in progressbar(enumerate(u.create_batches(list(page_titles_to_fetch),
                                                                          batch_size=batch_size)),
                                               max_value=int(len(page_titles_to_fetch)/batch_size)):
      batch_pages_to_process = _fetch_pages(pages_db, titles_batch)
      latest_processed_pages = _process_pages(redirects_lookup, batch_pages_to_process)
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

def _get_entity(redirects_lookup, link):
  link_destination = link['page']
  followed_redirect = redirects_lookup.get(link_destination)
  return _.upper_first(followed_redirect or link_destination)

def _sentence_to_link_contexts(redirects_lookup, page, sentence):
  page_title = page['title']
  contexts = {}
  if 'links' in sentence:
    for link in sentence['links']:
      if is_valid_link(link):
        link_text = link.get('text') or link['page']
        try:
          mention_offset = get_mention_offset(page['plaintext'], sentence['text'], link_text)
          entity = _get_entity(redirects_lookup, link)
          context = {'text': link_text,
                     'sentence': sentence['text'],
                     'offset': mention_offset,
                     'page_title': page_title,
                     'preredirect': _.upper_first(link['page'])}
          if entity in contexts:
            contexts[entity].append(context)
          else:
            contexts[entity] = [context]
        except ValueError:
          continue
  return contexts

def _sentence_to_link_contexts_reducer(redirects_lookup, page, contexts_acc, sentence):
  contexts = _sentence_to_link_contexts(redirects_lookup, page, sentence)
  if not _.is_empty(contexts):
    concat = lambda dest, src: dest + src if dest else src
    _.merge_with(contexts_acc, contexts, iteratee=concat)
  return contexts_acc

def get_link_contexts(redirects_lookup, page):
  '''link contexts is a dictionary from entity to mention details'''
  sections = page['sections']
  sentences = sum([section['sentences'] for section in sections if 'sentences' in section], [])
  sentences_from_tables = sum([[table['data'] for table in section['tables'][0] if table.get('data')] for section in sections if section.get('tables')],
                              [])
  all_sentences = sentences + sentences_from_tables
  return reduce(_.curry(_sentence_to_link_contexts_reducer)(redirects_lookup, page),
                all_sentences,
                {})

def _mention_overlaps(mentions, mention_to_check):
  '''does a mention overlap a mention in the list.'''
  mention_spans = [[mention['offset'],
                    mention['offset'] + len(mention['text'])] for mention in mentions]
  start = mention_to_check['offset']
  end = mention_to_check['offset'] + len(mention_to_check['text'])
  starts_inside_a_mention = any([start >= span[0] and start <= span[1] for span in mention_spans])
  ends_inside_a_mention = any([end >= span[0] and end <= span[1] for span in mention_spans])
  contains_a_mention = any([start <= span[0] and end >= span[1] for span in mention_spans])
  return starts_inside_a_mention or ends_inside_a_mention or contains_a_mention

def _apply_match_heuristic(page, link_contexts, to_match, entity):
  '''helper for defining heuristics for finding mentions of an entity'''
  matches = u.match_all(to_match, page['plaintext'])
  mentions = _.flatten(link_contexts.values())
  link_context = {entity: [{'text': to_match,
                            'offset': match_index,
                            'page_title': page['title'],
                            'preredirect': _.upper_first(entity)} for match_index in matches]}
  filtered_link_context = {entity: [mention for mention in link_context[entity] if not _mention_overlaps(mentions, mention)]}
  concat = lambda dest, src: _.uniq_by(dest + src, 'offset') if dest else src
  if not _.is_empty(filtered_link_context[entity]):
    return _.merge_with(link_contexts, filtered_link_context, iteratee=concat)
  else:
    return link_contexts

def _apply_exact_match_heuristic(page, link_contexts, entity_to_match):
  return _apply_match_heuristic(page, link_contexts, entity_to_match, entity_to_match)

def _page_title_exact_match_heuristic(page, link_contexts):
  '''look for an occurance of the page title'''
  return _apply_exact_match_heuristic(page, link_contexts, page['title'])

def _link_title_exact_match_heuristic(page, link_contexts):
  '''look for an occurance of the link anchor text'''
  link_titles = list(link_contexts.keys())
  return reduce(_.curry(_apply_exact_match_heuristic)(page),
                link_titles,
                link_contexts)

def _entity_for_each_page(page, link_contexts):
  '''make sure that each page has an entry in the dict'''
  return _.assign({page['title']: []}, link_contexts)

def _drop_overlapping_mentions_reducer(acc, pair):
  mentions_so_far, link_contexts = acc
  entity, mention = pair
  if not _mention_overlaps(mentions_so_far, mention):
    mentions_so_far.append(mention)
    u.append_at_key(link_contexts, entity, mention)
  return (mentions_so_far, link_contexts)

def _drop_overlapping_mentions(link_contexts):
  entity_mention_pairs = sum(_.map_values(link_contexts,
                                          lambda mentions, entity: [[entity, mention] for mention in mentions]).values(),
                             [])
  __, reduced_link_contexts = reduce(_drop_overlapping_mentions_reducer,
                                     entity_mention_pairs,
                                     ([], {}))
  return reduced_link_contexts

def get_link_contexts_using_heuristics(redirects_lookup, page):
  return pipe(get_link_contexts(redirects_lookup, page),
              _.partial(_page_title_exact_match_heuristic, page),
              _.partial(_link_title_exact_match_heuristic, page),
              _drop_overlapping_mentions,
              _.partial(_entity_for_each_page, page))

def process_page(redirects_lookup, page, is_seed_page=False):
  cleaned_page = clean_page(page)
  document_info = {'source_id': cleaned_page['pageID'],
                   'title': cleaned_page['title'],
                   'text': cleaned_page['plaintext'],
                   'categories': cleaned_page['categories'],
                   'is_disambiguation_page': cleaned_page['isDisambiguation'],
                   'is_seed_page': is_seed_page}
  link_contexts = get_link_contexts_using_heuristics(redirects_lookup, cleaned_page)
  entity_counts = _.map_values(link_contexts, len)
  return {'document_info': document_info,
          'link_contexts': link_contexts,
          'entity_counts': entity_counts}

def merge_mentions(processed_pages):
  '''merge the link contexts from a list of pages'''
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
