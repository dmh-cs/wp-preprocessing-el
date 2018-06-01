import pydash as _
from functools import reduce

from parsers import parse_for_sentence_offsets, parse_for_token_offsets

def _get_sentence(page_content, sentence_start, sentence_end):
  return page_content[sentence_start : sentence_end]

def _label_iobes(mention_start_end_offsets, token_start_end):
  [token_start, token_end] = token_start_end
  if any([token_start == mention_start and token_end == mention_end for mention_start, mention_end in mention_start_end_offsets]):
    return 'S'
  elif any([token_start == mention_start for mention_start, _ in mention_start_end_offsets]):
    return 'B'
  elif any([token_end == mention_end for _, mention_end in mention_start_end_offsets]):
    return 'E'
  elif any([token_start > mention_start and token_end < mention_end for mention_start, mention_end in mention_start_end_offsets]):
    return 'I'
  else:
    return 'O'

def _label_iobes_reducer(mention_start_end_offsets, sentence_iobes, token_start_end):
  sentence_iobes.append(_label_iobes(mention_start_end_offsets, token_start_end))
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

def _get_splice(start_end_offset, index):
  if index == start_end_offset[0] or index == start_end_offset[1]:
    return
  else:
    return [(start_end_offset[0], index), (index, start_end_offset[1])]

def _splice_at(start_end_offsets, index):
  for i, start_end_offset in enumerate(start_end_offsets):
    if index >= start_end_offset[0] and index <= start_end_offset[1]:
      splice = _get_splice(start_end_offsets[i], index)
      if splice:
        return start_end_offsets[:i] + splice + start_end_offsets[i + 1:]
      else:
        return start_end_offsets
  print('start_end_offsets:', start_end_offsets)
  print('Split index:', index)
  raise ValueError('Mention extends outside of detected tokens')

def _merge_start_end_offsets(start_end_offsets, start_end_offsets_to_merge):
  offsets = start_end_offsets
  for new_start_end in start_end_offsets_to_merge:
    new_start, new_end = new_start_end
    offsets = _splice_at(offsets, new_start)
    offsets = _splice_at(offsets, new_end)
  return offsets

def get_page_iobes(page, mentions, mention_link_titles):
  page_iobes = []
  page_content = page['content']
  page_tokens = []
  mention_start_end_offsets = [[mention['offset'],
                                mention['offset'] + len(mention['text'])] for mention in mentions]
  for sentence_start, sentence_end in parse_for_sentence_offsets(page_content):
    f = lambda group: group[0][0] >= sentence_start and group[0][1] <= sentence_end
    filtered = _.collections.filter_(zip(mention_start_end_offsets, mention_link_titles),
                                     f)
    sentence_mention_start_end_offsets = [offset for offset, title in filtered]
    sentence_mention_link_titles = [title for offset, title in filtered]
    sentence = _get_sentence(page_content, sentence_start, sentence_end)
    sentence_token_offsets = parse_for_token_offsets(sentence)
    page_tokens += sentence_token_offsets
    sentence_tokens = [sentence[start : end] for start, end in sentence_token_offsets]
    token_offsets = [[offset[0] + sentence_start,
                      offset[1] + sentence_start] for offset in sentence_token_offsets]
    try:
      all_offsets = _merge_start_end_offsets(token_offsets, sentence_mention_start_end_offsets)
    except ValueError:
      print('Page:', page['title'])
      print('Sentence:', sentence)
      print('Sentence mentions:', sentence_mention_link_titles)
      raise
    token_start_offsets = [offset[0] for offset in all_offsets]
    token_end_offsets = [offset[1] for offset in all_offsets]
    try:
      iobes_sequence = reduce(_.functions.curry(_label_iobes_reducer)(sentence_mention_start_end_offsets),
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
  with open('./out/' + page['title'] + '.iobes', 'w') as f:
    f.write('\n\n'.join(['\n'.join([' '.join(iobes) for iobes in sentence_iobes]) for sentence_iobes in page_iobes]))
