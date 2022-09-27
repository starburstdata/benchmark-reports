SELECT
    name
  , value
FROM environment_attributes
WHERE environment_id = :id
ORDER BY name, value
;
