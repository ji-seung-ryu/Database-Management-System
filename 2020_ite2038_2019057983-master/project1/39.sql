SELECT DISTINCT name
FROM CatchedPokemon,Trainer
WHERE Trainer.id = owner_id
GROUP BY pid
HAVING COUNT(*)>=2
ORDER BY name;
