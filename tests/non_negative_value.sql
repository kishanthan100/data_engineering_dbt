SELECT *
FROM 
{{ ref('bronze_sales') }}

WHERE quantity < 0