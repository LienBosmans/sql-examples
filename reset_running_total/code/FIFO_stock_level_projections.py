import duckdb # documentation available on https://duckdb.org/docs/api/python/overview


# Read data tables from csv
customer_orders = duckdb.read_csv('reset_running_total/data/customer_orders.csv')
production_orders = duckdb.read_csv('reset_running_total/data/production_orders.csv')
products = duckdb.read_csv('reset_running_total/data/products.csv')
stock = duckdb.read_csv('reset_running_total/data/stock.csv')


# Part 1: Initial lower bound calculations

part1 = duckdb.sql("""
with stock_transactions as (
    select 
        product_code,
        batch_number as reference_number,
        qty as produced_qty,
        0 as ordered_qty,
        0 as potential_waste_qty,
        production_date as transaction_date,
        0 as sort_order
    from 
        stock
),
production_transactions as (
    select
        product_code,
        production_order_number as reference_number,
        qty as produced_qty,
        0 as ordered_qty,
        0 as potential_waste_qty,
        production_date as transaction_date,
        1 as sort_order
    from 
        production_orders
),
customer_transactions as (
    select
        product_code,
        customer_order_number as reference_number,
        0 as produced_qty,
        qty as ordered_qty,
        0 as potential_waste_qty,
        delivery_date as transaction_date,
        2 as sort_order
    from 
        customer_orders
),
expired_stock_transactions as (
    select 
        product_code,
        concat('WASTE ',batch_number) as reference_number,
        0 as produced_qty,
        0 as ordered_qty,
        qty as potential_waste_qty,
        expiration_date as transaction_date,
        3 as sort_order
    from 
        stock
),
expired_production_transactions as (
    select
        po.product_code,
        concat('WASTE ',po.production_order_number) as reference_number,
        0 as produced_qty,
        0 as ordered_qty,
        po.qty as potential_waste_qty,
        po.production_date + p.shelf_life_days::int as transaction_date,
        4 as sort_order
    from 
        production_orders as po
        left join products as p
            on po.product_code = p.product_code
),
transactions as (
    select * from stock_transactions
    union all
    select * from production_transactions
    union all
    select * from customer_transactions
    union all
    select * from expired_stock_transactions
    union all
    select * from expired_production_transactions
),
add_running_totals_qty as (
    select
        *,
        sum(produced_qty) over (partition by product_code order by transaction_date, sort_order, reference_number) as RT_produced,
        sum(ordered_qty) over (partition by product_code order by transaction_date, sort_order, reference_number) as RT_ordered,
        sum(potential_waste_qty) over (partition by product_code order by transaction_date, sort_order, reference_number) as RT_potential_waste
    from
        transactions
),
add_lower_bounds as (
    select 
        *,
        case
            when 0 < min(RT_produced - RT_ordered) over (partition by product_code order by transaction_date, sort_order, reference_number)
                then 0
            else -min(RT_produced - RT_ordered) over (partition by product_code order by transaction_date, sort_order, reference_number)
        end as LB_RT_missed_sales,
        case
            when 0 < min(RT_ordered - RT_potential_waste) over (partition by product_code order by transaction_date, sort_order, reference_number)
                then 0
            else -min(RT_ordered - RT_potential_waste) over (partition by product_code order by transaction_date, sort_order, reference_number)
        end as LB_RT_waste
    from
        add_running_totals_qty

)

select * from add_lower_bounds
""")

duckdb.sql("select * from part1 where product_code = 'cupc01' order by transaction_date, sort_order, reference_number").show()


# Part 2: Recursive lower bound calculations

transactions_with_running_totals = duckdb.sql("""
with stock_transactions as (
    select 
        product_code,
        batch_number as reference_number,
        qty as produced_qty,
        0 as ordered_qty,
        0 as potential_waste_qty,
        production_date as transaction_date,
        0 as sort_order
    from 
        stock
),
production_transactions as (
    select
        product_code,
        production_order_number as reference_number,
        qty as produced_qty,
        0 as ordered_qty,
        0 as potential_waste_qty,
        production_date as transaction_date,
        1 as sort_order
    from 
        production_orders
),
customer_transactions as (
    select
        product_code,
        customer_order_number as reference_number,
        0 as produced_qty,
        qty as ordered_qty,
        0 as potential_waste_qty,
        delivery_date as transaction_date,
        2 as sort_order
    from 
        customer_orders
),
expired_stock_transactions as (
    select 
        product_code,
        concat('WASTE ',batch_number) as reference_number,
        0 as produced_qty,
        0 as ordered_qty,
        qty as potential_waste_qty,
        expiration_date as transaction_date,
        3 as sort_order
    from 
        stock
),
expired_production_transactions as (
    select
        po.product_code,
        concat('WASTE ',po.production_order_number) as reference_number,
        0 as produced_qty,
        0 as ordered_qty,
        po.qty as potential_waste_qty,
        po.production_date + p.shelf_life_days::int as transaction_date,
        4 as sort_order
    from 
        production_orders as po
        left join products as p
            on po.product_code = p.product_code
),
transactions as (
    select * from stock_transactions
    union all
    select * from production_transactions
    union all
    select * from customer_transactions
    union all
    select * from expired_stock_transactions
    union all
    select * from expired_production_transactions
),
add_running_totals_qty as (
    select
        *,
        sum(produced_qty) over (partition by product_code order by transaction_date, sort_order, reference_number) as RT_produced,
        sum(ordered_qty) over (partition by product_code order by transaction_date, sort_order, reference_number) as RT_ordered,
        sum(potential_waste_qty) over (partition by product_code order by transaction_date, sort_order, reference_number) as RT_potential_waste
    from
        transactions
)

select * from add_running_totals_qty
""")

recursive_lower_bounds = duckdb.sql("""
with recursive calculate_lower_bounds 
    (recursion_depth, product_code, reference_number, produced_qty, ordered_qty, potential_waste_qty,transaction_date, sort_order, RT_produced, RT_ordered, RT_potential_waste, prev_LB_RT_missed_sales, prev_LB_RT_waste, LB_RT_missed_sales, LB_RT_waste)
as (
    -- anchor
    select 
        0 as recursion_depth,
        product_code,
        reference_number,
        produced_qty,
        ordered_qty,
        potential_waste_qty,
        transaction_date,
        sort_order,
        RT_produced,
        RT_ordered,
        RT_potential_waste,
        -1 as prev_LB_RT_missed_sales,
        -1 as prev_LB_RT_waste,
        0 as LB_RT_missed_sales,
        0 as LB_RT_waste
    from
        transactions_with_running_totals
    UNION ALL
    -- recursive step
    select
        recursion_depth + 1 as recursion_depth,
        product_code,
        reference_number,
        produced_qty,
        ordered_qty,
        potential_waste_qty,
        transaction_date,
        sort_order,
        RT_produced,
        RT_ordered,
        RT_potential_waste,
        LB_RT_missed_sales as prev_LB_RT_missed_sales,
        LB_RT_waste as prev_LB_RT_waste,
        case
            when 0 < min(RT_produced - LB_RT_waste - RT_ordered) over (partition by product_code order by transaction_date, sort_order, reference_number)
                then 0
            else -min(RT_produced - LB_RT_waste - RT_ordered) over (partition by product_code order by transaction_date, sort_order, reference_number)
        end as LB_RT_missed_sales,
        case
            when 0 < min(RT_ordered - LB_RT_missed_sales - RT_potential_waste) over (partition by product_code order by transaction_date, sort_order, reference_number)
                then 0
            else -min(RT_ordered - LB_RT_missed_sales - RT_potential_waste) over (partition by product_code order by transaction_date, sort_order, reference_number)
        end as LB_RT_waste
    from
        calculate_lower_bounds
    where
        recursion_depth + 1 < 10 -- safeguards
        and product_code in 
            (
                select product_code 
                from calculate_lower_bounds 
                group by product_code 
                having 
                    count(case when LB_RT_missed_sales - prev_LB_RT_missed_sales > 0 then 1 end) > 0
                    or count(case when LB_RT_waste - prev_LB_RT_waste > 0 then 1 end) > 0
            )
)

select * from calculate_lower_bounds
""")

stock_levels = duckdb.sql(""" 
with max_recursion_depth as (
    select 
        product_code,
        max(recursion_depth) as max_recursion_depth
    from
        recursive_lower_bounds
    group by
        product_code
),
filtered_lower_bounds_result as (
    select
        recursive_lower_bounds.*
    from 
        recursive_lower_bounds
        inner join max_recursion_depth
            on (
                recursive_lower_bounds.product_code = max_recursion_depth.product_code
                and recursive_lower_bounds.recursion_depth = max_recursion_depth.max_recursion_depth
            )
),
calculate_stock_levels as (
    select
        product_code,
        reference_number,
        sort_order,
        transaction_date,
        produced_qty,
        ordered_qty,
        potential_waste_qty,
        ordered_qty - (LB_RT_missed_sales - lag(LB_RT_missed_sales) over (partition by product_code order by transaction_date, sort_order, reference_number)) as sold_qty,
        LB_RT_missed_sales - lag(LB_RT_missed_sales) over (partition by product_code order by transaction_date, sort_order, reference_number) as missed_sales_qty,
        LB_RT_waste - lag(LB_RT_waste) over (partition by product_code order by transaction_date, sort_order, reference_number) as waste_qty,
        RT_produced,
        RT_ordered,
        RT_potential_waste,
        RT_ordered - LB_RT_missed_sales as RT_sold,
        LB_RT_missed_sales as RT_missed_sales,
        LB_RT_waste as RT_waste,
        RT_produced - RT_ordered + LB_RT_missed_sales - LB_RT_waste as stock
    from
        filtered_lower_bounds_result
)
                          
select * from calculate_stock_levels
""")

duckdb.sql("select * from stock_levels where product_code = 'cupc01' order by transaction_date, sort_order, reference_number").show()
