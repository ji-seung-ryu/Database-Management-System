SELECT Pokemon.name, level, nickname
FROM CatchedPokemon
JOIN Gym ON leader_id = owner_id
JOIN Pokemon ON pid = Pokemon.id
WHERE nickname LIKE 'A%'
ORDER BY Pokemon.name DESC;