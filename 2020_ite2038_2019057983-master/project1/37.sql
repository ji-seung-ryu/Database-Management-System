SELECT Trainer.name, sum(level) AS sum2
FROM CatchedPokemon, Trainer
WHERE Trainer.id = owner_id
GROUP BY owner_id
HAVING sum2>=(
SELECT MAX(sum1)
FROM(
SELECT sum(level) AS sum1
FROM CatchedPokemon
GROUP BY owner_id) derived )
ORDER BY Trainer.name
