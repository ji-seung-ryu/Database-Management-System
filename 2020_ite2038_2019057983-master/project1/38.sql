SELECT name
FROM Evolution, Pokemon
WHERE after_id = pokemon.id AND after_id NOT IN (
  SELECT before_id
  FROM Evolution
)
ORDER BY name;