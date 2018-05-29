def fetch_disambiguation_pages(pages_db):
  return pages_db.find({"title": {"$regex": "(disambiguation)"}})

def main():
  raise NotImplementedError('Disambiguation pages might be used for canditate selection. This will likely require a new table in the mysql db')


if __name__ == "__main__": main()
