CREATE VIEW mention_by_entity AS SELECT e.text as entity, m.text as mention
       FROM mentions m INNER JOIN entity_mentions em ON m.id = em.mention_id
                       INNER JOIN entities e ON e.id = em.entity_id;
