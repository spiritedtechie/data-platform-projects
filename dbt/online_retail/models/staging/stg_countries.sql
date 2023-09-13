{{ config(
    materialized = 'table'
) }}

WITH nulls_replaced AS (

    SELECT
        id,
        iso,
        NAME,
        nicename AS nice_name,
        NULLIF(
            iso3,
            'NULL'
        ) AS iso3,
        NULLIF(
            numcode,
            'NULL'
        ) AS num_code,
        phonecode AS phone_code
    FROM
        {{ source(
            'raw_retail',
            'countries'
        ) }}
)
SELECT
    CAST(id AS INTEGER),
    CAST(iso AS VARCHAR(3)),
    CAST(NAME AS VARCHAR(50)),
    CAST(nice_name AS VARCHAR(50)),
    CAST(iso3 AS VARCHAR(3)),
    CAST(
        num_code AS INTEGER
    ),
    CAST(
        phone_code AS INTEGER
    )
FROM
    nulls_replaced
