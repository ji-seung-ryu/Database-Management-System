SELECT COUNT(DISTINCT type) AS CNT
FROM Pokemon
WHERE id IN(
SELECT pid
FROM CatchedPokemon
WHERE owner_id IN(
SELECT id
FROM Trainer
WHERE hometown = 'Sangnok City')
  )
  
