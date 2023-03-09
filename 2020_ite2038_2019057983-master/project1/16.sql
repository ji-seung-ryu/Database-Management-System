SELECT SUM(level)
FROM Pokemon
JOIN CatchedPokemon ON CatchedPokemon.pid = Pokemon.id
WHERE type = 'Water' OR type = 'Electric' OR type = 'Psychic'