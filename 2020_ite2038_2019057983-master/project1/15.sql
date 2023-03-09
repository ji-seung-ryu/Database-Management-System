SELECT owner_id, COUNT(*)
FROM CatchedPokemon
GROUP BY owner_id 
LIMIT 1;