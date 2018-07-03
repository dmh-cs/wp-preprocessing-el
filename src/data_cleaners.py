import re
import pydash as _
from functools import reduce

def _drop_template_parens(page_content):
  cleaned = re.sub(r'\( *[,;.][^()]* *\)', '', page_content)
  cleaned = re.sub(r'\( *[^()]*[,;] *\)', '', cleaned)
  return cleaned

def _drop_reference_tag(page_content):
  return page_content.replace('<references />', '')


def clean_page_content(page_content):
  return _drop_template_parens(_drop_reference_tag(page_content))

def _cleaned_link_is_valid(sentence_text, cleaned_link):
  link_text_is_in_page = 'text' in cleaned_link and sentence_text.find(cleaned_link['text']) != -1
  link_page_is_in_page = 'page' in cleaned_link and sentence_text.find(cleaned_link['page']) != -1
  link_mention_is_in_page = link_text_is_in_page or (link_page_is_in_page and 'text' not in cleaned_link)
  link_mention_is_blank = _.is_empty(cleaned_link['text'].strip()) if 'text' in cleaned_link else False
  link_page_is_blank = _.is_empty(cleaned_link['page'].strip()) if 'page' in cleaned_link else False
  return not link_page_is_blank and not link_mention_is_blank and link_mention_is_in_page

def _sentence_clean_link_text(link):
  if 'text' in link:
    cleaned_text = clean_page_content(link['text']).strip()
    return _.assign({}, link, {'text': cleaned_text})
  else:
    return link

def _sentence_clean_link_page(link):
  if 'page' in link:
    return _.assign({}, link, {'page': link['page'].strip()})
  else:
    return link

def _sentence_clean_reducer(sentence_text, cleaned_links, link):
  link_has_content = 'text' in link or 'page' in link
  if link_has_content:
    cleaned_link = _sentence_clean_link_page(_sentence_clean_link_text(link))
    if _cleaned_link_is_valid(sentence_text, cleaned_link):
      return cleaned_links + [cleaned_link]
    else:
      return cleaned_links
  else:
    return cleaned_links

def _clean_sentence(sentence):
  cleaned_sentence = _.assign({}, sentence, {'text': clean_page_content(sentence['text'])})
  if 'links' in sentence:
    cleaned_sentence['links'] = reduce(_.curry(_sentence_clean_reducer)(cleaned_sentence['text']),
                                       sentence['links'],
                                       [])
  return cleaned_sentence

def _clean_table(table):
  if 'data' in table:
    return _.assign({}, table, {'data': _clean_sentence(table['data'])})
  else:
    return table

def _clean_sentences(sentences):
  return [_clean_sentence(sentence) for sentence in sentences]

def _clean_tables(tables):
  return [[_clean_table(table) for table in tables[0]]]

def _clean_section(section):
  if 'sentences' in section:
    cleaned_section = {'sentences': _clean_sentences(section['sentences'])}
  else:
    cleaned_section = section
  if 'tables' in section:
    _.assign(cleaned_section, {'tables': _clean_tables(section['tables'])})
  return _.assign({}, section, cleaned_section)

def clean_page(page):
  cleaned_page = {'plaintext': clean_page_content(page['plaintext']),
                  'sections': [_clean_section(section) for section in page['sections']]}
  return _.assign({}, page, cleaned_page)
