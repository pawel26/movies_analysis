SELECT DISTINCT
    title,
    box_office
FROM
    ref('dim_movie')
WHERE box_office IS NOT NULL
ORDER BY
    box_office DESC
limit 1;