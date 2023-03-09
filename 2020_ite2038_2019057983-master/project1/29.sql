SELECT type,COUNT(*)
FROM CatchedPokemon
JOIN Pokemon ON Pokemon.id = CatchedPokemon.pid
GROUP BY type
