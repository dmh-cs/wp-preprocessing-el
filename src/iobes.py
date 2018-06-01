import pydash as _
from functools import reduce

from parsers import parse_for_sentence_offsets, parse_for_token_offsets

def _get_sentence(page_content, sentence_start, sentence_end):
  return page_content[sentence_start : sentence_end]

def _label_iobes_reducer(mention_start_end_offsets, sentence_iobes, token_start_end):
  [token_start, token_end] = token_start_end
  if any([token_start == mention_start and token_end == mention_end for mention_start, mention_end in mention_start_end_offsets]):
    sentence_iobes.append('S')
  elif any([token_start == mention_start for mention_start, _ in mention_start_end_offsets]):
    sentence_iobes.append('B')
  elif any([token_end == mention_end for _, mention_end in mention_start_end_offsets]):
    sentence_iobes.append('E')
  elif any([token_start > mention_start and token_end < mention_end for mention_start, mention_end in mention_start_end_offsets]):
    sentence_iobes.append('I')
  else:
    if not _.predicates.is_empty(sentence_iobes) and sentence_iobes[-1] in ['I', 'B']:
      print('Sentence IOBES so far:', sentence_iobes)
      raise ValueError('Inserting O after ' + sentence_iobes[-1] + ' will create an unbalanced IOBES string')
    sentence_iobes.append('O')
  return sentence_iobes

def _insert_link_titles_and_tokens(iobes_sequence, mention_link_titles, tokens):
  '''WARNING: Mutates `mention_link_titles`'''
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
  for _ in range(mention_ctr): mention_link_titles.pop(0)
  return with_link_titles

def get_page_iobes(page, mentions, mention_link_titles):
  mention_link_titles_remaining = mention_link_titles
  page_iobes = []
  page_content = page['content']
  page_tokens = []
  for sentence_start, sentence_end in parse_for_sentence_offsets(page_content):
    sentence = _get_sentence(page_content, sentence_start, sentence_end)
    sentence_token_offsets = parse_for_token_offsets(sentence)
    page_tokens += sentence_token_offsets
    sentence_tokens = [sentence[start : end] for start, end in sentence_token_offsets]
    token_offsets = [[offset[0] + sentence_start,
                      offset[1] + sentence_start] for offset in sentence_token_offsets]
    token_start_offsets = [offset[0] for offset in token_offsets]
    token_end_offsets = [offset[1] for offset in token_offsets]
    mention_start_end_offsets = [[mention['offset'],
                                  mention['offset'] + len(mention['text'])] for mention in mentions]
    try:
      iobes_sequence = reduce(_.functions.curry(_label_iobes_reducer)(mention_start_end_offsets),
                              zip(token_start_offsets, token_end_offsets),
                              [])
    except ValueError:
      print('Page:', page['title'])
      print('Sentence:', sentence)
      print('Mentions remaining:', mention_link_titles_remaining)
      raise
    iobes_with_link_titles_and_content = _insert_link_titles_and_tokens(iobes_sequence,
                                                                        mention_link_titles_remaining,
                                                                        sentence_tokens)
    page_iobes.append(iobes_with_link_titles_and_content)
  return page_iobes

def write_page_iobes(page, page_iobes):
  with open('./out/' + page['title'] + '.iobes', 'w') as f:
    f.write('\n\n'.join(['\n'.join([' '.join(iobes) for iobes in sentence_iobes]) for sentence_iobes in page_iobes]))
