with select_data as (
    select
        "CustomerID" as customer_id,
        "Gender" as gender,
        "Age" as age,
        "Annual Income ($)" as annual_income,
        "Spending Score (1-100)" as spending_score,
        "Profession" as profession,
        "Work Experience" as working_experience,
        "Family Size" as family_size
    from {{ref('stg_customers')}}

)

select * from select_data