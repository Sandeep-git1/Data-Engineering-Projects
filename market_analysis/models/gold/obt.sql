with
    f_sales as (select * from {{ ref("fact_sales_gold") }}),
    d_cust as (select * from {{ ref("dim_customer_gold") }}),
    d_prod as (select * from {{ ref("dim_product_gold") }})

select
    -- Fact Metrics & Dates
    f_sales.order_number,
    f_sales.order_date,
    f_sales.ship_date,
    f_sales.due_date,
    f_sales.quantity,
    f_sales.price,
    f_sales.sales_amount,

    -- Customer Details
    d_cust.customer_id,
    d_cust.first_name,
    d_cust.last_name,
    d_cust.marital_status,
    d_cust.gender,
    d_cust.country,

    -- Product Details
    d_prod.product_id,
    d_prod.product_name,
    d_prod.product_line,
    d_prod.category,
    d_prod.subcategory

from f_sales
left join d_cust on f_sales.customer_sk = d_cust.customer_sk
left join d_prod on f_sales.product_sk = d_prod.product_sk
