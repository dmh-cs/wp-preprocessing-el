CREATE TABLE entity_mentions_text_tbl AS
       SELECT mention,
              offset,
              mention_id,
              page_id,
              entity,
              entity_id
       FROM entity_mentions_text
       ORDER BY page_id;
