SELECT DISTINCT name, type
FROM Pokemon
WHERE id IN(
SELECT Pid
FROM CatchedPokemon
WHERE level>=30
)
ORDER BY name;