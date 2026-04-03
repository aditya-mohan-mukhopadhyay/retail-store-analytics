use demodb;
show tables;
create table retail_sales 
(  transaction_id  VARCHAR(15) ,
   customer_id   VARCHAR(15) ,
   customer_name  VARCHAR(25) ,
   customer_age  INT ,
   gender VARCHAR(15),
   product_id  VARCHAR(15) ,
   product_name  VARCHAR(15),
   product_category  VARCHAR(15),
   quantity INT,
   price FLOAT,
   payment_mode  VARCHAR(15),
   purchase_date  DATE,
   time_of_purchase TIME,
   order_status  VARCHAR(15),
   orderstatus  VARCHAR(30)) ;
   
   
   drop table retail_sales;
   select  * from retail_sales;

LOAD DATA INFILE 'D:/ExcelR data Analyst Course/Retail store analytics/sales_store_updated.csv'
INTO TABLE retail_sales
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    transaction_id, customer_id, customer_name, @customer_age,
    gender, product_id, product_name, product_category,
    @quantity, price, payment_mode, @purchase_date,
    time_of_purchase, order_status,orderstatus
)
SET 
    customer_age = NULLIF(@customer_age, ''),
    quantity = NULLIF(@quantity, ''),
    purchase_date = STR_TO_DATE(NULLIF(@purchase_date, ''), '%d/%m/%Y');
    
    select * from retail_sales;
    select * from retail_sales where orderstatus = 'pending';
    select * from retail_sales where product_name='sofa';
    select * from retail_sales where year(purchase_date)='2023';
    select * from retail_sales where customer_name='Ehsaan Ram';
    select orderstatus, count(quantity) as quantity from retail_sales
    where product_name='sofa'
    group by orderstatus;
    
    
select 
    orderstatus, 
    hex(orderstatus), 
    length(orderstatus)
from retail_sales
where orderstatus is not null
limit 10;

SET SQL_SAFE_UPDATES = 0;

update retail_sales
set orderstatus = trim(
    replace(
        replace(orderstatus, '\r', ''),
    '\n', '')
);

update retail_sales
set orderstatus = trim(
    replace(
        replace(
            replace(orderstatus, '\r', ''),
        '\n', ''),
    '\t', '')
);

select *  from retail_sales  where orderstatus = 'delivered';
    
ALTER TABLE retail_sales
drop column order_status;

select * from retail_sales;




    
    -- Data Cleaning
    
    -- Step 1:  Check for duplicates
    
  with cte as 
(  SELECT  *,
                      ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_id) as row_num
                      FROM 
                      retail_sales )
 
select * from cte
where row_num >1;
    
    with cte as 
(  SELECT  *,
                      ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_id) as row_num
                      FROM 
                      retail_sales )
 
select * from cte
where transaction_id IN ( 'TXN240646',' TXN342128', 'TXN855235', 'TXN981773');

-- Deleting Duplicate records

SET SQL_SAFE_UPDATES = 0;

select * from retail_sales;
select count(*) from retail_sales;
select * from retail_sales
where transaction_id IN ( 'TXN240646',' TXN342128', 'TXN855235', 'TXN981773');

-- Add primary key for deletion of duplicate rows

ALTER TABLE retail_sales 
ADD COLUMN row_id INT AUTO_INCREMENT PRIMARY KEY FIRST;

SELECT row_id, transaction_id
FROM retail_sales
WHERE transaction_id IN ('TXN240646', 'TXN342128', 'TXN855235', 'TXN981773')
ORDER BY transaction_id, row_id;

SET SQL_SAFE_UPDATES = 0;

DELETE FROM retail_sales
WHERE row_id NOT IN (
    SELECT min_id FROM (
        SELECT MIN(row_id) AS min_id
        FROM retail_sales
        GROUP BY transaction_id
    ) AS temp
);

SELECT transaction_id, COUNT(*) as count
FROM retail_sales
WHERE transaction_id IN ('TXN240646', 'TXN342128', 'TXN855235', 'TXN981773')
GROUP BY transaction_id;

SET SQL_SAFE_UPDATES = 1;




select * from retail_sales;
select count(*) from retail_sales;
SET SQL_SAFE_UPDATES = 1;

alter table retail_sales
drop column row_id;


-- Step 2 :  Check Data Type 

select column_name,  data_type
from information_schema.columns
where table_name= 'retail_sales';

-- Step 3 :  Check for Null Values

SELECT 'transaction_id'   AS column_name, SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END)   AS null_count FROM retail_sales UNION ALL
SELECT 'customer_id'      AS column_name, SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)      AS null_count FROM retail_sales UNION ALL
SELECT 'customer_name'    AS column_name, SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END)    AS null_count FROM retail_sales UNION ALL
SELECT 'customer_age'     AS column_name, SUM(CASE WHEN customer_age IS NULL THEN 1 ELSE 0 END)     AS null_count FROM retail_sales UNION ALL
SELECT 'gender'           AS column_name, SUM(CASE WHEN gender IS NULL THEN 1 ELSE 0 END)           AS null_count FROM retail_sales UNION ALL
SELECT 'product_id'       AS column_name, SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END)       AS null_count FROM retail_sales UNION ALL
SELECT 'product_name'     AS column_name, SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END)     AS null_count FROM retail_sales UNION ALL
SELECT 'product_category' AS column_name, SUM(CASE WHEN product_category IS NULL THEN 1 ELSE 0 END) AS null_count FROM retail_sales UNION ALL
SELECT 'quantity'         AS column_name, SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END)         AS null_count FROM retail_sales UNION ALL
SELECT 'price'            AS column_name, SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END)            AS null_count FROM retail_sales UNION ALL
SELECT 'payment_mode'     AS column_name, SUM(CASE WHEN payment_mode IS NULL THEN 1 ELSE 0 END)     AS null_count FROM retail_sales UNION ALL
SELECT 'purchase_date'    AS column_name, SUM(CASE WHEN purchase_date IS NULL THEN 1 ELSE 0 END)    AS null_count FROM retail_sales UNION ALL
SELECT 'time_of_purchase' AS column_name, SUM(CASE WHEN time_of_purchase IS NULL THEN 1 ELSE 0 END) AS null_count FROM retail_sales UNION ALL
SELECT 'orderstatus'           AS column_name, SUM(CASE WHEN orderstatus IS NULL THEN 1 ELSE 0 END)           AS null_count 
FROM retail_sales;

select * from retail_sales;

-- treating null values

SELECT * 
FROM retail_sales
WHERE 
    transaction_id IS NULL OR TRIM(transaction_id) = '' OR
    customer_id IS NULL OR TRIM(customer_id) = '' OR
    customer_name IS NULL OR TRIM(customer_name) = '' OR
    customer_age IS NULL OR
    gender IS NULL OR TRIM(gender) = '' OR
    product_id IS NULL OR TRIM(product_id) = '' OR
    product_name IS NULL OR TRIM(product_name) = '' OR
    product_category IS NULL OR TRIM(product_category) = '' OR
    quantity IS NULL OR
    price IS NULL OR
    payment_mode IS NULL OR TRIM(payment_mode) = '' OR
    purchase_date IS NULL OR
    time_of_purchase IS NULL OR
    orderstatus IS NULL OR TRIM(orderstatus) = '';

select count(*) from retail_sales;

select * from retail_sales;

select * from retail_sales where TRIM(order_status) = 'delivered';
select product_name, order_status from retail_sales;

SET SQL_SAFE_UPDATES = 0;

delete from retail_sales
where transaction_id IS NULL  OR TRIM(transaction_id) = '';

select * from retail_sales
where customer_name ='Ehsaan Ram';

update retail_sales
set customer_id = 'CUST9494'
where transaction_id = 'TXN977900';

select * from retail_sales
where customer_name ='Damini Raju';

update retail_sales
set customer_id = 'CUST1401'
where transaction_id = 'TXN985663';

select * from retail_sales  where customer_id = 'CUST1003';

update retail_sales
set customer_name = 'Mahika Saini', customer_age=35, gender='male'
where transaction_id = 'TXN432798';

select * from retail_sales;
select count(*) from retail_sales;

-- Step 4  Data Cleaning

select distinct gender from retail_sales;

update retail_sales
set gender='M'
where gender='male';

update retail_sales
set gender='F'
where gender='female';

select distinct payment_mode from retail_sales;

update retail_sales
set payment_mode='Credit Card'
where payment_mode='CC';

-- Data Analysis
-- solving business problems

select * from retail_sales;

-- 1 . What are the top 5 most selling products by quantity ?

select product_name, sum(quantity) as total_quantity_sold
from retail_sales
where orderstatus = 'delivered'
group by product_name
order by total_quantity_sold desc
limit 5; 

-- 2. Which products are mostly cancelled ?

select product_name, count(*) as total_cancelled_products
from retail_sales
where orderstatus = 'cancelled'
group by product_name
order by total_cancelled_products desc
limit 5;

-- 3 . What time of day has highest number of  purchases ?

select 
          case 
                   when extract(hour from time_of_purchase) between 0 and 5 then 'night'
                   when extract(hour from time_of_purchase) between 6 and 11 then 'morning'
                   when extract(hour from time_of_purchase) between 12 and 17 then 'afternoon'
                   when extract(hour from time_of_purchase) between 18 and 24 then 'evening'
			end as time_of_day,
            count(*) as total_orders
   from retail_sales
  group by
           case 
                   when extract(hour from time_of_purchase) between 0 and 5 then 'night'
                   when extract(hour from time_of_purchase) between 6 and 11 then 'morning'
                   when extract(hour from time_of_purchase) between 12 and 17 then 'afternoon'
                   when extract(hour from time_of_purchase) between 18 and 24 then 'evening'
             end
       order by total_orders desc;
       
       -- 4. Who are the top 5 highest spending customers ?
       
      SELECT customer_name, 
       CONCAT('₹ ', FORMAT(SUM(quantity * price), 0)) AS total_spend
FROM retail_sales
GROUP BY customer_name
ORDER BY SUM(quantity * price) DESC
LIMIT 5;

-- 5.  Which product categories generate the highest revenue ?

SELECT product_category, 
       CONCAT('₹ ', FORMAT(SUM(quantity * price), 0)) AS total_spend
FROM retail_sales
GROUP BY product_category
ORDER BY SUM(quantity * price) DESC
LIMIT 5;

-- 6. What is the return /cancellation rate per product category ?

-- cancellation

SELECT product_category,
       CONCAT(
           ROUND(
               COUNT(CASE WHEN orderstatus = 'cancelled' THEN 1 END) * 100.0 
               / COUNT(*), 
           2),
       '%') AS cancelled_percent
FROM  retail_sales
GROUP BY product_category
ORDER BY cancelled_percent DESC; 

-- returned

SELECT product_category,
       CONCAT( ROUND( COUNT(CASE WHEN orderstatus = 'returned' THEN 1 END) * 100.0 
		/ COUNT(*),  2),
       '%') AS cancelled_percent
FROM  retail_sales
GROUP BY product_category
ORDER BY cancelled_percent DESC; 

       
select * from retail_sales;


-- 7.  What is the most preffered payment mode ?

select payment_mode,count(*) as number_of_payments
from retail_sales
group by payment_mode
order by number_of_payments desc;

-- 8. How does age group affect purcashing behaviour ?

select 
           case 
                   when customer_age between 18 and 25 then '18-25'
                   when customer_age between 26 and 35 then '26-35'
                   when customer_age between 36 and 50 then '36-50'
                   else '51+'
                   end as customer_age,
                   CONCAT('₹ ', FORMAT(SUM(quantity * price), 0))  as total_purchase
             from retail_sales      
           group by  
                      case 
                   when customer_age between 18 and 25 then '18-25'
                   when customer_age between 26 and 35 then '26-35'
                   when customer_age between 36 and 50 then '36-50'
                   else '51+'
                   end
              order by total_purchase desc;
              
     -- 9.  What is the monthly sales trend ?
     
     select  year( purchase_date ) as year,
                 month( purchase_date) as month,
                 CONCAT('₹ ', FORMAT(SUM(quantity * price), 0))  as total_sales,
                 sum(quantity) as total_quantity
       from retail_sales
       group by  year( purchase_date ), month( purchase_date)
       order by month;
       
       -- 10. Are certain genders buying more specific product categories ?
       
       select gender, product_category, count(product_category) as total_purchase
       from retail_sales
       group by gender, product_category 
       order by product_category, total_purchase desc; 
       
       
       -- If we want Male and Female in separate columns 
       -- Conditional aggregation
       
	SELECT product_category,
    SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS male,
    SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS female
FROM retail_sales
GROUP BY product_category
ORDER BY product_category;
                   