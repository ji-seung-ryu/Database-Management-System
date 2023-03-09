SELECT Trainer.name
FROM Trainer
JOIN CatchedPokemon ON Trainer.id = CatchedPokemon.owner_id
GROUP BY name
HAVING COUNT(*)>=3
ORDER BY COUNT(*) DESC; 