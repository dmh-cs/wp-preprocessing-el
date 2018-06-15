import re
import pydash as _

def _drop_template_parens(page_content):
  cleaned = re.sub(r'\( *[,;.][^()]* *\)', '', page_content)
  cleaned = re.sub(r'\( *[^()]*[,;] *\)', '', cleaned)
  return cleaned

def _drop_reference_tag(page_content):
  return page_content.replace('<references />', '')


def clean_page_content(page_content):
  return _drop_template_parens(_drop_reference_tag(page_content))

def _clean_sentence(sentence):
  cleaned_sentence = _.assign({}, sentence, {'text': clean_page_content(sentence['text'])})
  if sentence.get('links'):
    cleaned_links = []
    for link in sentence['links']:
      if link.get('text'):
        cleaned_text = clean_page_content(link['text'])
        if cleaned_sentence['text'].find(cleaned_text) != -1:
          cleaned_links.append(_.assign({}, link, {'text': cleaned_text}))
    cleaned_sentence['links'] = cleaned_links
  return cleaned_sentence

def _clean_table(table):
  if table.get('data'):
    return _.assign({}, table, {'data': _clean_sentence(table['data'])})
  else:
    return table

def _clean_sentences(sentences):
  return [_clean_sentence(sentence) for sentence in sentences]

def _clean_tables(tables):
  return [[_clean_table(table) for table in tables[0]]]

def _clean_section(section):
  cleaned_section = {'sentences': _clean_sentences(section['sentences'])}
  if section.get('tables'):
    _.assign(cleaned_section, {'tables': _clean_tables(section['tables'])})
  return _.assign({}, section, cleaned_section)

def clean_page(page):
  cleaned_page = {'plaintext': clean_page_content(page['plaintext']),
                  'sections': [_clean_section(section) for section in page['sections']]}
  return _.assign({}, page, cleaned_page)
