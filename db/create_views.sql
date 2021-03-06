CREATE OR REPLACE VIEW mention_by_entity AS
       SELECT m.text AS text,
              e.text AS entity,
              m.offset AS offset,
              m.page_id AS page_id,
              m.preredirect AS preredirect
       FROM mentions m INNER JOIN entity_mentions em ON m.id = em.mention_id
                       INNER JOIN entities e ON e.id = em.entity_id;

CREATE OR REPLACE VIEW category_by_page AS SELECT p.id AS page_id, c.category AS category
       FROM categories c INNER JOIN page_categories pc ON c.id = pc.category_id
                         INNER JOIN pages p ON p.id = pc.page_id;

CREATE OR REPLACE VIEW entity_by_page AS
       SELECT e.id AS entity_id,
              p.id AS page_id,
              p.source_id as source_id,
              e.text AS entity
       FROM entities e INNER JOIN pages p ON p.title = e.text;

CREATE OR REPLACE VIEW entity_mentions_text AS
       SELECT m.text AS mention,
              m.offset AS offset,
              m.id AS mention_id,
              m.page_id AS page_id,
              e.text AS entity,
              e.id AS entity_id
              FROM mentions m INNER JOIN entity_mentions em ON em.mention_id = m.id
                              INNER JOIN entities e on em.entity_id = e.id;
