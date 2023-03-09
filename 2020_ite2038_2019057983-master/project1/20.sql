SELECT name, COUNT(name) AS CNT
FROM CatchedPokemon
JOIN Trainer ON hometown = 'Sangnok City' AND owner_id= Trainer.id
GROUP BY name
ORDER BY COUNT(name) 