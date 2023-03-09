SELECT name
FROM Pokemon
WHERE Pokemon.id NOT IN(
  SELECT pid
  FROM CatchedPokemon
  )
  ORDER BY name;