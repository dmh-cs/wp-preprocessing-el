import pydash as _

def build_cursor_generator(cursor, buff_len=1000):
  while True:
    results = cursor.fetchmany(buff_len)
    if not results: return
    for result in results: yield result

def sort_mentions(mentions):
  return _.arrays.sort(mentions, key=lambda obj: obj['offset'])
