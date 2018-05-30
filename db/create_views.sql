CREATE VIEW mention_by_entity AS SELECT e.text as entity, m.text as mention
       FROM mentions m INNER JOIN entity_mentions em ON m.id = em.mention_id
                       INNER JOIN entities e ON e.id = em.entity_id;

CREATE VIEW category_by_page AS SELECT p.id as page_id, c.category as category
       FROM categories c INNER JOIN page_categories pc ON c.id = pc.category_id
                       INNER JOIN pages p ON p.id = pc.page_id;
