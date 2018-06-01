import pydash as _

from parsers import parse_for_sentence_offsets, parse_for_token_offsets

def _get_sentence(page_content, sentence_start, sentence_end):
  return page_content[sentence_start : sentence_end]

def _label_iobes(mention_start_end_offsets, token_start, token_end):
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

def _insert_link_titles_and_tokens(iobes_sequence, mention_link_titles, tokens):
  '''WARNING: Mutates `mention_link_titles`'''
  with_link_titles = []
  mention_ctr = 0
  for token, iobes in zip(tokens, iobes_sequence):
    row = []
    row.append(token)
    row.append(iobes)
    if iobes == 'E' or iobes == 'S':
      row.append(mention_link_titles[mention_ctr])
      mention_ctr += 1
    with_link_titles.append(row)
  _.arrays.drop(mention_link_titles, mention_ctr)
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
    iobes_sequence = [_label_iobes(mention_start_end_offsets, start, end) for start, end in zip(token_start_offsets,
                                                                                                     token_end_offsets)]
    iobes_with_link_titles_and_content = _insert_link_titles_and_tokens(iobes_sequence,
                                                                        mention_link_titles_remaining,
                                                                        sentence_tokens)
    page_iobes.append(iobes_with_link_titles_and_content)
  return page_iobes

def write_page_iobes(page, page_iobes):
  with open('./out/' + page + '.iobes', 'w') as f:
    f.write('\n'.join(page_iobes))
