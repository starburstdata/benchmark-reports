-- Env details
-- Reads the names and values of all environment attributes associated with a specific environment.
SELECT
    name
  , value
FROM environment_attributes
WHERE environment_id = :id
ORDER BY name, value
;
