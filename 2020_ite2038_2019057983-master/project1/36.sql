SELECT name
FROM Evolution, CatchedPokemon, Trainer
WHERE after_id = pid AND owner_id = Trainer.id AND after_id NOT IN (
  SELECT before_id
  FROM Evolution
)
ORDER BY name;