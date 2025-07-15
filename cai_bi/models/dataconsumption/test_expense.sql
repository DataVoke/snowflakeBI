{{
    config(
        schema="consolidation",
        alias="alias_test_expense",
    )
}}
select 
    * 
from {{ ref('expense') }}