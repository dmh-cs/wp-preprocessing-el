def clean_page_content(page_content):
  return page_content.replace('<references />', '').strip()
