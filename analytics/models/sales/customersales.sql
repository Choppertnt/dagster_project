with 
    test_data1 as (

        select * from {{ ref('sql_employee') }}
    ),
    test_data2 as (
        select * from {{ ref('sql_career') }}
    ),
    test_data_all as (
        select role,sum(sales) from (
        select distinct test_data1.*,role from test_data1
        left join test_data2 ON
        test_data1.id = test_data2.id
        where sales>1900
        )
        group by role

    )
select * from test_data_all