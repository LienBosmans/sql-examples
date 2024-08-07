import duckdb # documentation available on https://duckdb.org/docs/api/python/overview


# Read data tables from csv
customer_orders = duckdb.read_csv('reset_running_total/data/customer_orders.csv')
production_orders = duckdb.read_csv('reset_running_total/data/production_orders.csv')
products = duckdb.read_csv('reset_running_total/data/products.csv')
stock = duckdb.read_csv('reset_running_total/data/stock.csv')


# Question 1: can we fill the orders?
result1 = duckdb.sql("""
with stock_transactions as (
    select 
        product_code,
        batch_number as reference_number,
        qty,
        production_date as transaction_date,
        0 as sort_order
    from 
        stock
),
production_transactions as (
    select
        product_code,
        production_order_number as reference_number,
        qty,
        production_date as transaction_date,
        0 as sort_order
    from 
        production_orders
),
customer_transactions as (
    select
        product_code,
        customer_order_number as reference_number,
        -qty as qty,
        delivery_date as transaction_date,
        1 as sort_order
    from 
        customer_orders
),
transactions as (
    select * from stock_transactions
    union all
    select * from production_transactions
    union all
    select * from customer_transactions
),
running_total_v1 as (
    select 
        product_code,
        reference_number,
        qty,
        transaction_date,
        sort_order,
        sum(qty) over (partition by product_code order by transaction_date, sort_order) as running_total_v1
    from
        transactions
),
min_running_total_v1 as (
    select
        product_code,
        reference_number,
        qty,
        transaction_date,
        sort_order,
        running_total_v1,
        min(running_total_v1) over (partition by product_code order by transaction_date, sort_order) as min_running_total_v1
    from
        running_total_v1
),
running_total_v2 as (
    select
        product_code,
        reference_number,
        qty,
        transaction_date,
        sort_order,
        running_total_v1,
        min_running_total_v1,
        case 
            when min_running_total_v1 >= 0 then running_total_v1
            else running_total_v1 - min_running_total_v1
        end as running_total_v2
    from
        min_running_total_v1
)

select * from running_total_v2
order by product_code, transaction_date, sort_order
""")

duckdb.sql("select * from result1 where product_code = 'cupc01'").show()


# Question 2: Will it expire?
result2 = duckdb.sql("""
with stock_transactions as (
    select 
        product_code,
        batch_number as reference_number,
        -qty as qty,
        expiration_date as transaction_date,
        1 as sort_order
    from 
        stock
),
production_transactions as (
    select
        po.product_code,
        po.production_order_number as reference_number,
        -po.qty as qty,
        po.production_date + p.shelf_life_days::int as transaction_date,
        1 as sort_order
    from 
        production_orders as po
        left join products as p
            on po.product_code = p.product_code
),
customer_transactions as (
    select
        product_code,
        customer_order_number as reference_number,
        qty,
        delivery_date as transaction_date,
        0 as sort_order
    from 
        customer_orders
),
transactions as (
    select * from stock_transactions
    union all
    select * from production_transactions
    union all
    select * from customer_transactions
),
running_total_v1 as (
    select 
        product_code,
        reference_number,
        qty,
        transaction_date,
        sort_order,
        sum(qty) over (partition by product_code order by transaction_date, sort_order) as running_total_v1
    from
        transactions
),
min_running_total_v1 as (
    select
        product_code,
        reference_number,
        qty,
        transaction_date,
        sort_order,
        running_total_v1,
        min(running_total_v1) over (partition by product_code order by transaction_date, sort_order) as min_running_total_v1
    from
        running_total_v1
),
running_total_v2 as (
    select
        product_code,
        reference_number,
        qty,
        transaction_date,
        sort_order,
        running_total_v1,
        min_running_total_v1,
        case 
            when min_running_total_v1 >= 0 then running_total_v1
            else running_total_v1 - min_running_total_v1
        end as running_total_v2
    from
        min_running_total_v1
)

select * from running_total_v2
order by product_code, transaction_date, sort_order
""")

duckdb.sql("select * from result2 where product_code = 'cupc01'").show()


# Question 3: Which stock will expire?

result3 = duckdb.sql("""
with running_total_expired_stock as (
    select
        *,
        case
            when min_running_total_v1 < 0 then min_running_total_v1
            else 0
        end as running_total_expired_stock
    from 
        result2
),
expired_stock_transactions as (
    select 
        product_code,
        reference_number,
        running_total_expired_stock - lag(running_total_expired_stock) over (partition by product_code order by transaction_date, sort_order) as qty,
        transaction_date
    from 
        running_total_expired_stock
)

select * from expired_stock_transactions
where qty is not null and qty <> 0
""")

duckdb.sql("select * from result3 where product_code = 'cupc01'").show()


# Question 4: Calculate the correct running total

result4 = duckdb.sql("""
with stock_transactions as (
    select 
        product_code,
        batch_number as reference_number,
        qty,
        production_date as transaction_date,
        0 as sort_order
    from 
        stock
),
production_transactions as (
    select
        product_code,
        production_order_number as reference_number,
        qty,
        production_date as transaction_date,
        0 as sort_order
    from 
        production_orders
),
customer_transactions as (
    select
        product_code,
        customer_order_number as reference_number,
        -qty as qty,
        delivery_date as transaction_date,
        1 as sort_order
    from 
        customer_orders
),
expired_stock_transactions as (
    select
        product_code,
        reference_number,
        qty as qty,
        transaction_date,
        2 as sort_order
    from 
        result3
),
transactions as (
    select * from stock_transactions
    union all
    select * from production_transactions
    union all
    select * from customer_transactions
    union all
    select * from expired_stock_transactions
),
running_total_v1 as (
    select 
        product_code,
        reference_number,
        qty,
        transaction_date,
        sort_order,
        sum(qty) over (partition by product_code order by transaction_date, sort_order) as running_total_v1
    from
        transactions
),
min_running_total_v1 as (
    select
        product_code,
        reference_number,
        qty,
        transaction_date,
        sort_order,
        running_total_v1,
        min(running_total_v1) over (partition by product_code order by transaction_date, sort_order) as min_running_total_v1
    from
        running_total_v1
),
running_total_v2 as (
    select
        product_code,
        reference_number,
        qty,
        transaction_date,
        sort_order,
        running_total_v1,
        min_running_total_v1,
        case 
            when min_running_total_v1 >= 0 then running_total_v1
            else running_total_v1 - min_running_total_v1
        end as running_total_v2,
    from
        min_running_total_v1
)

select
        product_code,
        reference_number,
        qty,
        transaction_date,
        running_total_v2 as running_total
from 
    running_total_v2
order by
    product_code, transaction_date, sort_order
""")

duckdb.sql("select * from result4 where product_code = 'cupc01'").show()

