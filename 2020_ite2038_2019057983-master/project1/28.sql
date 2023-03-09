SELECT Trainer.name, AVG(level)
FROM Trainer
JOIN CatchedPokemon ON CatchedPokemon.owner_id = Trainer.id
JOIN Pokemon ON CatchedPokemon.pid= Pokemon.id AND (Pokemon.type = 'Normal' OR Pokemon.type= 'Electric')
GROUP BY Trainer.name
ORDER BY AVG(level);
