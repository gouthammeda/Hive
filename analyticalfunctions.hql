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
select employee_id,department_id,salary from employees limit 10;

--calculating the sum of salaries for each department
select department_id,sum(salary) department_salary_expense
from employees
group by department_id;

--get total department wise salary expense(i.e., new column) for every employee within each department and order them using department_id ascending order. 
--It will use two map reduce jobs one for calculating salary expense and another for ordering the data.
select e.employee_id,e.department_id,e.salary,
    sum(e.salary) over (partition by e.department_id) department_salary_expense
from employees e
order by department_id;

--Result:
e.employee_id   e.department_id e.salary    department_salary_expense
178 NULL    7000.00 7000.00
200 10  4400.00 4400.00
202 20  6000.00 19000.00
201 20  13000.00    19000.00
115 30  3100.00 24900.00
114 30  11000.00    24900.00
116 30  2900.00 24900.00
117 30  2800.00 24900.00
118 30  2600.00 24900.00
119 30  2500.00 24900.00


--for each employee get both department wise salary expense and percentage of employee salary expense within the department and order them with department id, similarly we can do avg,min or max as well.
select e.employee_id,e.department_id,e.salary,
    sum(e.salary) over (partition by e.department_id) department_salary_expense,
    round((e.salary/sum(e.salary) over (partition by e.department_id)) * 100,2) percent_expense
from employees e
order by department_id;

--result:
e.employee_id   e.department_id e.salary    department_salary_expense   percent_expense
178 NULL    7000.00 7000.00 100.00
200 10  4400.00 4400.00 100.00
202 20  6000.00 19000.00    31.58 --6000 is 31.58% of 19000
201 20  13000.00    19000.00    68.42
115 30  3100.00 24900.00    12.45
114 30  11000.00    24900.00    44.18
116 30  2900.00 24900.00    11.65
117 30  2800.00 24900.00    11.24
118 30  2600.00 24900.00    10.44
119 30  2500.00 24900.00    10.04


use gouthamkumar_retail;
--It generates the revenue for each date or daily.
--we are creating table by grouping on order_date and selecting order_dates and sum of order_item_subtotal as revenue by joining orders with 
--order_items and filtering those orders which are complete and closed
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
--we are creating table by grouping on order_date,order_item_product_id and selecting order_date,order_item_product_id and sum of order_item_subtotal as revenue by joining orders with 
--order_items and filtering those orders which are complete and closed

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

--typically lead or lag are used to get preceding or following record from current record.
--inorder to get following date we use the lead function if rows are not specified then default is 1
--result should contain current date and revenue with following date and revenue.
select t.*,
  lead(order_date) over (order by order_date desc) as prior_date,
  lead(revenue) over (order by order_date desc) as prior_revenue
from daily_revenue t limit 10;

--try and understand lag query.
-- select t.*,
--   lead(order_date) over (order by order_date desc) as prior_date,
--   lag(revenue) over (order by order_date) as prior_revenue
-- from daily_revenue t limit 10;

--using lead function for getting results of date with next 7 days and replacing last 7 rows with 0 instead of null
--where there is no order_date or prior revenue.

select t.*,
  lead(order_date,7) over (order by order_date desc) as prior_date,
  lead(revenue,7,0) over (order by order_Date desc) as prior_revenue
from daily_revenue t;

--if we want to perform lead or lag with partition by clause within a key to group data and get following or preceding records
--for each date getting top performing product and next performing product in the same row 
select t.*,
   lead(order_item_product_id) over(partition by order_date order by revenue desc) next_order_item_product_id,
   lead(revenue) over (partition by order_date order by revenue desc) next_revenue
from daily_product_revenue t 
limit 100;


--as we are using lead function to get the top performing product we have to sort descending order by revenue also we are 
--replacing null with 0's and getting 1 record preceding.
--try performing the same with lag query or how lag can be used in same query.
select t.*,
   lead(order_item_product_id) over(partition by order_date order by revenue desc) next_order_item_product_id,
   lead(revenue,1,0) over (partition by order_date order by revenue desc) next_revenue
from daily_product_revenue t 
limit 100;

--using first value to get highest revenue generating product id and revenue for each date 
--we are using partition by order_date as key to group data and order by revenue desc 
select t.*,
    first_value(order_item_product_id) over (partition by order_date order by revenue desc) first_order_item_product_id,
    first_value(revenue) over (partition by order_date order by revenue desc) first_revenue 
from daily_product_revenue t 
limit 100;

--using last value to get highest revenue generating product id and revenue for each date 
--solve more examples.
select t.*,
    last_value(order_item_product_id) over(partition by order_date order by revenue 
        rows between current row and unbounded following) last_order_item_product_id,
    last_value(revenue) over(
        partition by order_date order by revenue 
        rows between current row and unbounded following) last_revenue
from daily_product_revenue as t 
order by order_date, revenue desc 
limit 100;

--rank func to calculate the rank for each product based on revenue grouped on order_date column. 
select t.*,
    rank() over (partition by order_date order by revenue desc) as rnk 
from daily_product_revenue t 
order by order_date, revenue desc 
limit 100;

--in rank func to handle duplicates, we assign same rank to product for same salary and next skipped rank is given to next product.
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

2013-07-25 00:00:00.0   1004    5599.72 1
2013-07-25 00:00:00.0   191 5099.49 2
2013-07-25 00:00:00.0   957 4499.7  3
2013-07-25 00:00:00.0   365 3359.44 4
2013-07-25 00:00:00.0   1073    2999.85 5
2013-07-25 00:00:00.0   1014    2798.88 6
2013-07-25 00:00:00.0   403 1949.85 7
2013-07-25 00:00:00.0   502 1650.0  8
2013-07-25 00:00:00.0   627 1079.73 9
2013-07-25 00:00:00.0   226 599.99  10

--in dense rank func to handle duplicates, we assign same rank to product for same salary and next rank is given to next product.
select employee_id,
       department_id,
       salary,
       dense_rank() over (partition by department_id order by salary desc) rnk
from employees 
order by department_id,salary desc;

193 50  3900.00 9
188 50  3800.00 10
137 50  3600.00 11
189 50  3600.00 11
141 50  3500.00 12

--row_number func to calculate rank for product based on revenue for each date
select t.*,
    row_number() over (partition by order_date order by revenue desc) as rn
from daily_product_revenue t 
order by order_date, revenue desc
limit 100; 

t.order_date    t.order_item_product_id t.revenue   rn
2013-07-25 00:00:00.0   1004    5599.72 1
2013-07-25 00:00:00.0   191 5099.49 2
2013-07-25 00:00:00.0   957 4499.7  3
2013-07-25 00:00:00.0   365 3359.44 4
2013-07-25 00:00:00.0   1073    2999.85 5
2013-07-25 00:00:00.0   1014    2798.88 6
2013-07-25 00:00:00.0   403 1949.85 7
2013-07-25 00:00:00.0   502 1650.0  8
2013-07-25 00:00:00.0   627 1079.73 9
2013-07-25 00:00:00.0   226 599.99  10


--in row_num func to handle duplicates, we assign unique or different ranks to product for same salary.
--we shouldn't use it for ranking purpose if it has duplicates.
use gouthamkumar_hr;
select 
    employee_id,department_id,salary,
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
--if the salaries are same rank and dense rank will assign the same rank but row number will assign different rank
--after it the rank will will skip the current rank and assign next rank but dense rank will not skip the current 
--rank and assign the same rank.
select 
    employee_id,department_id,salary,
    rank() over (partition by department_id order by salary desc) as rnk,
    dense_rank() over (partition by department_id order by salary desc) as drnk,
    row_number() over (partition by department_id order by salary desc) as rn
from employees 
order by department_id,salary desc;

203 40  6500.00 1   1   1
121 50  8200.00 1   1   1
120 50  8000.00 2   2   2
122 50  7900.00 3   3   3
123 50  6500.00 4   4   4
124 50  5800.00 5   5   5
184 50  4200.00 6   6   6
185 50  4100.00 7   7   7
192 50  4000.00 8   8   8
193 50  3900.00 9   9   9
188 50  3800.00 10  10  10
137 50  3600.00 11  11  11
189 50  3600.00 11  11  12
141 50  3500.00 13  12  13
186 50  3400.00 14  13  14

--select from join on where group by having order by 
--order of execution different from typing  
--first from clause will be run to get all the fields of orders then grouping of data occurs on that grouped data count occurs then sorting of data is done finishes with limit.
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

/* order of exec is first within from clause join and filter happens then grouping occurs then revenue is
calc in select clause for each product followed by grouping of data
we can only access revenue as part of order clause only but not in where or join class 
here data is first sorted by order_date ascending and then revenue descending.*/
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
    select order_date,count(1) as order_count 
    from orders 
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

--result:
2013-07-25 00:00:00.0   1004    5599.72 1
2013-07-25 00:00:00.0   191 5099.49 2
2013-07-25 00:00:00.0   957 4499.7  3
2013-07-25 00:00:00.0   365 3359.44 4
2013-07-25 00:00:00.0   1073    2999.85 5


--2nd highest revenue generated product on july 29th 2013 
select * from(
select t.*,
    rank() over (partition by order_date order by revenue desc) as rnk 
from daily_product_revenue t
) q
where rnk=2 and q.order_date="2013-07-29 00:00:00.0";

--result
2013-07-29 00:00:00.0   365 6658.89 2

use gouthamkumar_hr;
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

--result 
abc 100000  3




