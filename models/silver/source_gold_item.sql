WITH dedup_query AS(
    SELECT *,
            ROW_NUMBER() OVER(PARTITION BY id ORDER BY updateDate) as dedupc_id
    FROM {{ source('source', 'item') }}
)

SELECT 
    id,
    name,
    catogory,
    updateDate,
    dedupc_id
FROM dedup_query
WHERE dedupc_id = 1