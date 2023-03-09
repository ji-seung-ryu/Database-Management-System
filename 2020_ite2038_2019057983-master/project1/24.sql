SELECT hometown, AVG(level)
FROM CatchedPokemon
JOIN Trainer ON owner_id = Trainer.id
GROUP BY hometown
ORDER BY AVG(level);