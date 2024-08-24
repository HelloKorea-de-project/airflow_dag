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
    {{ source('raw_data', 'user_info') }}