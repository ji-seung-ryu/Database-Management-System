SELECT AVG(level)
FROM CatchedPokemon
JOIN Trainer ON Trainer.name = 'Red' AND owner_id = Trainer.id;
