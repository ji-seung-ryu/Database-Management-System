SELECT AVG(level)
FROM CatchedPokemon
JOIN Trainer ON Trainer.hometown = 'Sangnok City'
JOIN Pokemon 
WHERE Trainer.id = CatchedPokemon.owner_id AND CatchedPokemon.pid = Pokemon.id AND Pokemon.type = 'Electric';