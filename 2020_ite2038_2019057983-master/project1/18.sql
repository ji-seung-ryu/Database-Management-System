SELECT AVG(level)
FROM CatchedPokemon
WHERE owner_id IN(
SELECT leader_id
FROM Gym
  );
  
