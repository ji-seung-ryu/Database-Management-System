SELECT name, COUNT(*)
FROM CatchedPokemon
JOIN Gym ON owner_id = leader_id
JOIN Trainer ON owner_id = Trainer.id
GROUP BY name

