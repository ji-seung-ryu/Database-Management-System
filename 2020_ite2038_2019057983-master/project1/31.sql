SELECT type
FROM Pokemon
JOIN Evolution ON before_id = Pokemon.id 
GROUP BY type
HAVING COUNT(*)>=3
ORDER BY type DESC;