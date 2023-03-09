SELECT name
FROM Pokemon
WHERE Pokemon.id IN(
SELECT before_id
FROM Evolution
WHERE before_id>after_id)
ORDER BY name;