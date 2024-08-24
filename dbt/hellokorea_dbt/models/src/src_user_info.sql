WITH src_user_info AS(
    SELECT * FROM raw_data.user_info
)
SELECT
    user_id,
    country,
    pass_num,
    name,
    age,
    gender,
    email,
    registration_date
FROM
    src_user_info