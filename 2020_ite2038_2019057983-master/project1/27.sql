SELECT name,max(level)
FROM Trainer
JOIN CatchedPokemon ON CatchedPokemon.owner_id= Trainer.id
GROUP BY name 
HAVING COUNT(*) >= 4
ORDER BY name;