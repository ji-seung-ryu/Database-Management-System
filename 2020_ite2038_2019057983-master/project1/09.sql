SELECT name, AVG(level)
FROM Trainer
JOIN CatchedPokemon
WHERE Trainer.id = CatchedPokemon.owner_id
GROUP BY name;
