--ssh itv000613@g01.itversity.com u4onzoojuje9rf88fwo4dlfz6vinhe0r

--Analytics or windowing functions:
create database gouthamkumar_hr location 'hdfs://m01.itversity.com:9000/user/itv000613/db/hr';
use gouthamkumar_hr;

CREATE TABLE employees( 
employee_id INTEGER
, first_name VARCHAR(20)
, last_name VARCHAR(25)
, email VARCHAR(25)
, phone_number VARCHAR(20)
, hire_date DATE
, job_id VARCHAR(10)
, salary NUMERIC(8,2)
, commission_pct NUMERIC(2,2)
, manager_id INTEGER
, department_id INTEGER
) row format delimited fields terminated by '\t';

load data local inpath '/data/hr_db/employees' into table employees;
set hive.cli.print.header=true;
select * from employees limit 10;

select department_id,sum(salary) department_salary_expense
from employees
group by department_id;

--get total department wise salary expense for each employee. 
select e.employee_id,e.department_id,e.salary,
       sum(e.salary) over (partition by e.department_id) department_salary_expense
from employees e;
order by department_id;

--get the percentage of employee salary within the whole department, similarly you can do avg as well.
select e.employee_id,e.department_id,e.salary,
    sum(e.salary) over (partition by e.department_id) department_salary_expense,
    round((e.salary/sum(e.salary) over (partition by e.department_id))*100,2) pct_expense
from employees e
order by department_id;

use gouthamkumar_retail;
--It generates the revenue for each date or daily.
create table daily_revenue
as 
select o.order_date,
       round(sum(oi.order_item_subtotal),2) as revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id 
where o.order_status IN ('COMPLETE','CLOSED')
group by o.order_date;

select * from daily_revenue limit 10;

--generating daily revenue for each product 
create table daily_product_revenue
as 
select o.order_date,
       oi.order_item_product_id,
       round(sum(oi.order_item_subtotal),2) as revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id 
where o.order_status IN ('COMPLETE','CLOSED')
group by o.order_date,oi.order_item_product_id;

select * from daily_product_revenue limit 10;
select * from daily_revenue order by order_date desc limit 10;

--inorder to get following date we use the lead function if rows are not specified then default is 1
select t.*,
  lead(order_date) over (order by order_date desc) as prior_date,
  lead(revenue) over (order by order_Date desc) as prior_revenue
from daily_revenue t limit 10;

--using lead function for getting results of next 7 days
select t.*,
  lead(order_date,7) over (order by order_date desc) as prior_date,
  lead(revenue,7,0) over (order by order_Date desc) as prior_revenue
from daily_revenue t;

--for each date getting top performing product and next performing product 
select t.*,
   lead(order_item_product_id) over(partition by order_date order by revenue desc) next_order_item_product_id,
   lead(revenue) over (partition by order_date order by revenue desc) next_revenue
from daily_product_revenue t 
limit 100;

--replacing null with 0's and getting next 1 row
select t.*,
   lead(order_item_product_id) over(partition by order_date order by revenue desc) next_order_item_product_id,
   lead(revenue,1,0) over (partition by order_date order by revenue desc) next_revenue
from daily_product_revenue t 
limit 100;

--using first value to get highest revenue generating product id and revenue for each date 
select t.*,
    first_value(order_item_product_id) over (partition by order_date order by revenue desc) first_order_item_product_id,
    first_value(revenue) over (partition by order_date order by revenue desc) first_revenue 
from daily_product_revenue t 
limit 100;

--come back later 
--using last value to get highest revenue generating product id and revenue for each date 
select t.*,
    last_value(order_item_product_id) over(partition by order_date order by revenue 
        rows between current row and unbounded following) last_order_item_product_id,
    last_value(revenue) over(
        partition by order_date order by revenue 
        rows between current row and unbounded following 
        ) last_revenue
from daily_product_revenue as t 
order by order_date, revenue desc 
limit 100;

--rank func to calculate the rank for product based on revenue for each date 
select t.*,
    rank() over (partition by order_date order by revenue desc) as rnk 
from daily_product_revenue t 
order by order_date, revenue desc 
limit 100;

--rank func to handle duplicates, we assign same rank to product for same salary and next skipped rank is given to next product.
use gouthamkumar_hr; 
select
    employee_id,
    department_id,
    salary,
    rank() over (partition by department_id order by salary desc) rnk 
from employees 
order by department_id, salary desc;  

--dense rank func to calculate rank for product based on revenue for each date 
select t.*,
   dense_rank() over (partition by order_date order by revenue desc) as rnk 
from daily_product_revenue t 
order by order_date, revenue desc 
limit 100;

--in dense rank func to handle duplicates, we assign same rank to product for same salary and next rank is given to next product.
select employee_id,
       department_id,
       salary,
       dense_rank() over (partition by department_id order by salary desc) rnk
from employees 
order by department_id,salary desc;

--row_number func to calculate rank for product based on revenue for each date
select t.*,
    row_number() over (partition by order_date order by revenue desc) as rn
from daily_product_revenue t 
order by order_date, revenue desc
limit 100; 

--in row_num func to handle duplicates, we assign unique or different ranks to product for same salary.
--we shouldn't use it for ranking purpose if it has duplicates.
select 
    employee_id,
    department_id,
    salary,
    row_number() over (partition by department_id order by salary desc) as rn
from employees 
order by department_id, salary desc;

--combining all the three functions for daily_product_revenue 
select t.*,
    rank() over (partition by order_date order by revenue desc) as rnk,
    dense_rank() over (partition by order_date order by revenue desc) as drnk,
    row_number() over (partition by order_date order by revenue desc) as rn
from daily_product_revenue as t
order by order_date,revenue desc 
limit 100;

--combining all the three functions for employees 
select 
    employee_id,
    department_id,
    salary,
    rank() over (partition by department_id order by salary desc) as rnk,
    dense_rank() over (partition by department_id order by salary desc) as drnk,
    row_number() over (partition by department_id order by salary desc) as rn
from employees 
order by department_id,salary desc;

--order of execution different from typing  
--first from clause will be run then grouping of data occurs on that grouped data count occurs then sorting of data is done finishes with limit.
select order_date, count(1)
from orders 
group by order_date 
order by order_date 
limit 10;

--joining orders with order items within from clause then grouping to generate daily revenue for order_date and order by order_date cols. 
select o.order_date,
   round(sum(oi.order_item_subtotal),2) as revenue
from orders o join order_items oi 
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE','CLOSED')
group by o.order_date
order by o.order_date
limit 10;

/* order of exec is first within from clause join and filter happens then grouping occurs then revenue is calc in select clause for each product followed by grouping of data.
we can only access revenue as part of order clause only but not in where or join class */
select o.order_date,
    oi.order_item_product_id,
    round(sum(oi.order_item_subtotal),2) as revenue
from orders o join order_items oi 
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE','CLOSED')
group by o.order_date,oi.order_item_product_id
order by o.order_date,revenue desc 
limit 10;

--nested subquery, inner query just acts an another table 
select * from (select current_date)q;
select current_date from (select current_date)q;

-- we are getting the results of inner query and apply limit to get top 10 rows
select * from (
    select order_date,count(1) as order_count from orders 
    group by order_date
)q
limit 10;

--getting results on inner query and filtering orders greater than 200. 
select q.order_date,q.order_count from (
    select order_date,count(1) as order_count from orders 
    group by order_date
)q
where q.order_count>200;

--filtering data using fields derived from analytic functions
select * from (
select t.*,
    rank() over (partition by order_date order by revenue desc) as rnk 
from daily_product_revenue t
) q
where rnk <=5  
order by q.order_date, q.revenue desc 
limit 100;

--2nd highest revenue generated product on july 29th 2013 
select * from(
select t.*,
    rank() over (partition by order_date order by revenue desc) as rnk 
from daily_product_revenue t
) q
where rnk=2 and q.order_date="2013-07-29 00:00:00.0";

create table employee(
name varchar(50),
salary int
);

insert into employee values('abc',100000);
insert into employee values('bcd',1000000);
insert into employee values('efg',40000);
insert into employee values('ghi',500000);

--getting 3rd highest employee salary. if department_id or order_date exists then partition by that column or else not needed.
select * from (select e.*,
    rank() over (order by salary desc) as rnk 
from employee e) q
where rnk=3;




