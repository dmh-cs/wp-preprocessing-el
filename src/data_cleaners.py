import re

def _drop_template_parens(page_content):
  cleaned = re.sub(r'\( *[,;.][^()]* *\)', '', page_content)
  cleaned = re.sub(r'\( *[^()]*[,;] *\)', '', cleaned)
  return cleaned

def _drop_reference_tag(page_content):
  return page_content.replace('<references />', '')


def clean_page_content(page_content):
  return _drop_template_parens(_drop_reference_tag(page_content)).strip()
