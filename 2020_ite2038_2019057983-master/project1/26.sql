SELECT Pokemon.name
FROM CatchedPokemon
JOIN Pokemon ON CatchedPokemon.pid = Pokemon.id
WHERE nickname NOT LIKE '% %'
ORDER BY Pokemon.name DESC