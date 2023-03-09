SELECT COUNT(nickname) AS Pokemon_cnt
FROM Trainer
JOIN CatchedPokemon ON CatchedPokemon.owner_id =Trainer.id
WHERE hometown = 'Sangnok City'
  

