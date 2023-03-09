
SELECT DISTINCT Pokemon.name
FROM CatchedPokemon
JOIN Trainer ON hometown ='Sangnok City' AND owner_id = Trainer.id
JOIN Pokemon ON Pokemon.id = CatchedPokemon.pid
WHERE CatchedPokemon.pid in(
SELECT CatchedPokemon.pid
FROM CatchedPokemon
JOIN Trainer ON hometown ='Brown City' AND owner_id = Trainer.id
)
ORDER BY Pokemon.name