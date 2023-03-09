SELECT p1.id,p1.name AS 1st,p2.name AS 2nd,p3.name AS 3rd
FROM Evolution AS e2
JOIN Evolution AS e1 ON e1.after_id= e2.before_id
JOIN Pokemon AS p1 ON P1.id = e1.before_id
JOIN Pokemon AS p2 ON P2.id = e1.after_id
JOIN Pokemon AS p3 ON P3.id = e2.after_id
WHERE e2.before_id IN
(SELECT after_id
FROM Evolution
 )
 ORDER BY id;