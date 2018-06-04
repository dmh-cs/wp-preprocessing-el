import pydash as _
from functools import reduce
import re

from parsers import parse_for_sentence_spans, parse_for_token_spans

def _get_sentence(page_content, sentence_start, sentence_end):
  return page_content[sentence_start : sentence_end]

def _label_iobes(mention_spans, token_span):
  [token_start, token_end] = token_span
  if any([token_start == mention_start and token_end == mention_end for mention_start, mention_end in mention_spans]):
    return 'S'
  elif any([token_start == mention_start for mention_start, _ in mention_spans]):
    return 'B'
  elif any([token_end == mention_end for _, mention_end in mention_spans]):
    return 'E'
  elif any([token_start > mention_start and token_end < mention_end for mention_start, mention_end in mention_spans]):
    return 'I'
  else:
    return 'O'

def _label_iobes_reducer(mention_spans, sentence_iobes, token_span):
  sentence_iobes.append(_label_iobes(mention_spans, token_span))
  if len(sentence_iobes) > 1 and sentence_iobes[-2] in ['I', 'B'] and sentence_iobes[-1] == 'O':
    print('Sentence IOBES so far:', sentence_iobes)
    raise ValueError('Inserting O after ' + sentence_iobes[-1] + ' will create an unbalanced IOBES string')
  return sentence_iobes

def _insert_link_titles_and_tokens(iobes_sequence, mention_link_titles, tokens):
  with_link_titles = []
  mention_ctr = 0
  for token, iobes in zip(tokens, iobes_sequence):
    row = []
    row.append(iobes)
    row.append(token)
    if iobes != 'O':
      row.append(mention_link_titles[mention_ctr])
      if iobes == 'S' or iobes == 'E':
        mention_ctr += 1
    with_link_titles.append(row)
  return with_link_titles

def _get_splice(span, index):
  if index == span[0] or index == span[1]:
    return
  else:
    return [(span[0], index), (index, span[1])]

def _splice_at(spans, index):
  for i, span in enumerate(spans):
    if index >= span[0] and index <= span[1]:
      splice = _get_splice(spans[i], index)
      if splice:
        return spans[:i] + splice + spans[i + 1:]
      else:
        return spans
  raise ValueError('spans:', spans, '\n',
                   'Split index:', index, '\n',
                   'Mention extends outside of detected tokens')

def _is_mention_between_tokens(spans, span):
  mention_starts_at_token_end = any([span[0] == end for _, end in spans])
  mention_ends_at_token_start = any([span[1] == start for start, _ in spans])
  no_token_starts_at_mention_start = not any([span[0] == start for start, _ in spans])
  no_token_ends_at_mention_end = not any([span[1] == end for _, end in spans])
  return (mention_starts_at_token_end and no_token_starts_at_mention_start) or \
    (mention_ends_at_token_start and no_token_ends_at_mention_end)

def _merge_spans(spans, spans_to_merge):
  offsets = spans
  dropped_mention_indexes = []
  for i, new_span in enumerate(spans_to_merge):
    mention_is_between_tokens = _is_mention_between_tokens(offsets, new_span)
    if mention_is_between_tokens:
      dropped_mention_indexes.append(i)
    new_start, new_end = new_span
    try:
      offsets = _splice_at(offsets, new_start)
      offsets = _splice_at(offsets, new_end)
    except ValueError:
      dropped_mention_indexes.append(i)
  return offsets, _.uniq(dropped_mention_indexes)

def get_page_iobes(page, mentions, mention_link_titles):
  page_iobes = []
  page_content = page['content']
  page_tokens = []
  mention_spans = [[mention['offset'],
                    mention['offset'] + len(mention['text'])] for mention in mentions]
  for sentence_start, sentence_end in parse_for_sentence_spans(page_content):
    f = lambda group: group[0][0] >= sentence_start and group[0][1] <= sentence_end
    filtered = _.collections.filter_(zip(mention_spans, mention_link_titles),
                                     f)
    sentence_mention_spans = [offset for offset, title in filtered]
    sentence_mention_link_titles = [title for offset, title in filtered]
    sentence = _get_sentence(page_content, sentence_start, sentence_end)
    sentence_token_offsets = parse_for_token_spans(sentence)
    page_tokens += sentence_token_offsets
    sentence_tokens = [sentence[start : end] for start, end in sentence_token_offsets]
    token_spans = [[offset[0] + sentence_start,
                    offset[1] + sentence_start] for offset in sentence_token_offsets]
    try:
      all_spans, dropped_indexes = _merge_spans(token_spans, sentence_mention_spans)
      _.pull_at(sentence_mention_spans, dropped_indexes)
      _.pull_at(sentence_mention_link_titles, dropped_indexes)
    except ValueError:
      print('Page:', page['title'])
      print('Sentence:', sentence)
      print('Sentence mentions:', sentence_mention_link_titles)
      raise
    token_start_offsets = [offset[0] for offset in all_spans]
    token_end_offsets = [offset[1] for offset in all_spans]
    try:
      iobes_sequence = reduce(_.functions.curry(_label_iobes_reducer)(sentence_mention_spans),
                              zip(token_start_offsets, token_end_offsets),
                              [])
    except ValueError:
      print('Page:', page['title'])
      print('Sentence:', sentence)
      print('Sentence mentions:', sentence_mention_link_titles)
      raise
    iobes_with_link_titles_and_content = _insert_link_titles_and_tokens(iobes_sequence,
                                                                        sentence_mention_link_titles,
                                                                        sentence_tokens)
    page_iobes.append(iobes_with_link_titles_and_content)
  return page_iobes

def write_page_iobes(page, page_iobes):
  with open('./out/' + re.sub(r'\W+', '', page['title']) + '.iobes', 'w') as f:
    f.write(page['title'] + '\n' + '\n\n'.join(['\n'.join([' '.join(iobes) for iobes in sentence_iobes]) for sentence_iobes in page_iobes]))
