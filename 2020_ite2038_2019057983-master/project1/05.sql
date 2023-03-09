SELECT name
FROM Trainer
WHERE Trainer.name NOT IN(
SELECT name
FROM Trainer, GYM
WHERE Trainer.id = Gym.leader_id
  )
  ORDER BY name;