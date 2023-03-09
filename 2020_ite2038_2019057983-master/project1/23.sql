SELECT name
FROM Trainer
WHERE id IN (
  SELECT owner_id
  FROM CatchedPokemon
  WHERE level<=10
  )
  GROUP BY name
  ORDER BY name;