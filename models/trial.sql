{{ config(materialized='table') }}

with nt as (

    select * from new_table

)

select *
from nt