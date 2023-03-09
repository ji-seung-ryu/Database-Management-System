SELECT Pokemon.name, Pokemon.id
FROM CatchedPokemon
JOIN Pokemon ON Pokemon.id = CatchedPokemon.pid
WHERE owner_id in(
SELECT id 
FROM Trainer
WHERE hometown ='Sangnok City')
ORDER BY Pokemon.id;