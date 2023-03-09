SELECT hometown, MAX(level)
FROM Trainer, CatchedPokemon
WHERE CatchedPokemon.owner_id= Trainer.id
GROUP BY hometown;