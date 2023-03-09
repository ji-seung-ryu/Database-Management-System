SELECT type,COUNT(*)
FROM Pokemon
GROUP BY type
ORDER BY COUNT(*), name;