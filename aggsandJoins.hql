--ssh -o ServerAliveInterval=600 itv000613@g01.itversity.com
-- touch ~/.ssh/config
-- chmod 600 ~/.ssh/config
-- echo "ServerAliveInterval 600" >> ~/.ssh/config

--ssh itv000613@g01.itversity.com u4onzoojuje9rf88fwo4dlfz6vinhe0r
use gouthamkumar_retail;
hive --database gouthamkumar_retail


--we have to understand orders,order_items and products tables
--In order_items order_item_subtotal = order_item_quantity * order_item_product_price

--writing basic queries
drop table orders;

CREATE TABLE orders (
  order_id STRING COMMENT 'Unique order id',
  order_date STRING COMMENT 'Date on which order is placed',
  order_customer_id INT COMMENT 'Customer id who placed the order',
  order_status STRING COMMENT 'Current status of the order'
) COMMENT 'Table to save order level details'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/data/retail_db/orders' into table orders;

--select from join where group by(used for aggregations) having(filter on top of aggregated results) order by 
--once syntax and sematic checks are done for hql then it will get converted into mr(java based jar) job.

--logs will be present in /tmp/training folder 

select * from orders limit 10;
select order_id,order_customer_id from orders limit 10;

--grouping the order_item_order_id and generating order revenue
select order_item_order_id,sum(order_item_subtotal) as order_revenue
  from order_items
  group by order_item_order_id
  limit 10;

select order_id,order_customer_id from orders limit 10;

--we are calculating order_item_revenue(income generated from selling the product) for each of the product.
select order_item_order_id,order_item_product_id,
    order_item_quantity * order_item_product_price as order_item_revenue
     from order_items
     limit 10;

--we are generating another derived column actual_status using case and populating it based on the condition provided
select o.*,
 CASE 
 WHEN o.order_status IN('COMPLETE','CLOSED') THEN 'COMPLETED'
 WHEN o.order_status IN('PENDING','PENDING_PAYMENT','PAYMENT_REVIEW','PROCESSING') THEN 'PENDING'
 ELSE 'OTHER'
 END AS actual_status
from orders o limit 10;

--using distinct will give total unique values for the column.
select distinct order_date from orders;
select distinct order_status from orders;
select distinct order_item_product_id from order_items;
--it will return count of distinct values.
select count(distinct order_date) from orders;
--always run distinct when it is really needed as it will trigger the map reduce job but simple select does something like hadoop fs -cat.
select distinct * from orders limit 10;
select * from orders limit 10;

--Filtering data using conditions for string and numbers.
select * from orders where order_status='CLOSED' limit 10;
select * from orders where order_status='COMPLETE' limit 10;
SELECT * FROM orders WHERE order_customer_id = 8827;
select * from order_items where order_item_quantity >= 2 limit 10;
select * from order_items where order_item_subtotal >= 100 limit 10;
select * from orders where order_status !='COMPLETE' limit 10;
select * from orders where order_status <> 'COMPLETE' limit 10; 

--filter data using boolean operators or & and.
select * from orders where order_status='COMPLETE' and order_date = '2013-07-25 00:00:00.0' LIMIT 10;
select COUNT(1) from orders where order_status='COMPLETE' and order_date = '2013-07-25 00:00:00.0';
select * from orders where order_status='COMPLETE' OR order_date = '2013-07-25 00:00:00.0' LIMIT 10;

--below query returns 23000 records 
select count(1) from orders where order_status='COMPLETE' OR order_date = '2013-07-25 00:00:00.0';
--below query returns 22899 records
select count(1) from orders where order_status='COMPLETE';
--to validate we write below query which are not complete and orderdate is july 25th equals 101.
SELECT COUNT(1) FROM ORDERS where order_status <> 'COMPLETE' and order_date = '2013-07-25 00:00:00.0';

--using IN operator instead of or for comapring multiple values within single column
select * from orders 
 where order_status = 'COMPLETE' or order_status='CLOSED'
 LIMIT 10;

select * from orders 
 where order_status in('COMPLETE','CLOSED','PENDING')
 LIMIT 100;

SELECT * FROM ORDERS 
 WHERE ORDER_STATUS <> 'COMPLETE' AND ORDER_STATUS <> 'CLOSED'
 LIMIT 10;

--using like operator in where clause.
select * from orders 
where order_date like '2014%'
limit 10;

SELECT * FROM ORDERS 
  WHERE ORDER_DATE LIKE '%-07-%'
  limit 10;
--get count of dates for the month of july 
SELECT ORDER_DATE,COUNT(1) FROM ORDERS 
  WHERE ORDER_DATE LIKE '%-07-%'
  GROUP BY ORDER_DATE;

--come out of shell
exit;

--aggregation function
select count(1) from orders;
select count(distinct order_date) from orders;
--It will get distinct order_date and order_status 
select count(distinct order_date,order_status) from orders;

select * from order_items limit 10;
select * from order_items where order_item_order_id = 2;
--to use agg functions without group by clause.
select sum(order_item_subtotal) as order_revenue,
       min(order_item_subtotal) as min_order_item_subtotal,
       max(order_item_subtotal) as max_order_item_subtotal,
       avg(order_item_subtotal) as avg_order_item_subtotal
from order_items
where order_item_order_id = 2;

--using agg with group_by_key
-- select group_key1,group_key2,agg1(arg),agg2(arg)
-- from table_name 
-- group by group_key1,group_key2;
select order_item_order_id,
       sum(order_item_subtotal) order_revenue,
       min(order_item_subtotal) min_order_item_subtotal,
       max(order_item_subtotal) max_order_item_subtotal,
       avg(order_item_subtotal) avg_order_item_subtotal,
       count(order_item_subtotal) cnt_order_item_subtotal
from order_items
group by order_item_order_id
limit 10;

--we will get distinct count of order status from orders table for a given order date.
select order_date,count(distinct order_status) distinct_order_status_count
from orders
group by order_date
limit 10;

--columns in the select clause which are not part of aggregate functions should be in group by clause
--if there are more fields in group by than in select, results will be misleading it will give one record for each and every order_status which is incorrect.
select order_date,count(distinct order_status) distinct_order_status_count
from orders
group by order_date,order_status
limit 10;

--select from where group by having order by, if we want to filter after grouping then we have to use having instead 
--of where clause.limit is the optional keyword in below query  
select order_item_order_id,sum(order_item_subtotal) as order_revenue
from order_items
group by order_item_order_id
having sum(order_item_subtotal)>=500
limit 10;


--order by is used for global sorting and sort by is used to sort within a key
--by default sorting is done in ascending order.
select * from orders order by order_customer_id limit 10;
--Here we are using composite sorting which means data will sort by order_customer_id first and 
--if there are multiple records for a same order_customer_id then they will be sorted by order_date within them.
select * from orders 
order by order_customer_id,order_date
limit 10;

--composite sorting with order_date in descending order.In order by all the sorting is done by one reducer for that key on huge amount of data(rows)
--so performance can be slow to avoid it we can use sort by key
select * from orders 
order by order_customer_id asc, order_date desc
limit 10;

--hive query Execution life Cycle: 
--map tasks -> filtering and row level transformations
--reduce tasks -> aggregations,sorting,reduce side joins
--map tasks -> shuffle(data will be partitioned into fewer buckets depending on number of reducers used to process it(grouping). Also data will be sorted within each bucket(sorting)) -> reduce tasks 

--1 map(read the order_item_order_id and order_item_subtotal) and reduce task(added all order_item_subtotals for that order_item_order_id to compute revenue) and in shuffle stage data is grouped or distributed with order_item_order_id
select order_item_order_id,sum(order_item_subtotal) order_revenue
from order_items
group by order_item_order_id
limit 10;
--http://m02.itversity.com:19088/proxy/application_1658918988971_65369/

--always use the leading key as part of the distribute by clause in case of composite group by keys
--where global sort is not needed for example data sorted within sate in us but all the states are not required to sort in ascending order then we can say distribute by state and sort by some other condition, local sorting will use multiple reducers when compared with 
--global sorting so performance will be better.If we want to change field during group by then using distribute by we can do it.  
select order_item_order_id,sum(order_item_subtotal) order_revenue
from order_items
group by order_item_order_id
distribute by order_item_order_id
limit 10;

--sort by is alternative to order by which is useful to sort data within groups which are obtained by distribute by 
create database goutham_nyse location 'hdfs://m01.itversity.com:9000/user/itv000613/db/nyse';
use goutham_nyse;

create table stocks_eod(
 stockticker STRING,
 tradedate int,
 openprice float,
 highprice float,
 lowprice float,
 closeprice float,
 volume bigint
 ) row format delimited fields terminated by ',';

 load data local inpath '/data/nyse_all/nyse_data' into table stocks_eod;

create table stocks_eod_orderby (
 stockticker STRING,
 tradedate int,
 openprice float,
 highprice float,
 lowprice float,
 closeprice float,
 volume bigint
 ) row format delimited fields terminated by ',';

truncate table stocks_eod_orderby;

set mapreduce.job.reduces=8;
insert into stocks_eod_orderby
select * from stocks_eod
order by tradedate,volume desc;


--when we use sort by data we need to use distribute by or else data will be randomly distributed.
create table stocks_eod_sortby (
 stockticker STRING,
 tradedate int,
 openprice float,
 highprice float,
 lowprice float,
 closeprice float,
 volume bigint
 ) row format delimited fields terminated by ',';

--if we want to use sort within a key then distribute by and sort by have to be used instead of order by.
--we want to sort the data the data by tradedate then we have to mention it in the sort by or else data will be skewed.
--for same set of data sort by key completes quickly than order by key because number of reducers are more.
insert into table stocks_eod_sortby
select * from stocks_eod
distribute by tradedate sort by tradedate,volume desc;
 
describe formatted stocks_eod_sortby;
hadoop fs -get hdfs://m01.itversity.com:9000/user/itv000613/db/nyse/stocks_eod_sortby .

--if we want to use single column to distribute by and perform sort within same key then we can replace both with one cluster by which sorts data in ascending order
--but we cant change the sorting order.
drop table stocks_eod_clusterby;
create table stocks_eod_clusterby
row format delimited fields terminated by ','
as 
select * from stocks_eod
cluster by tradedate; 

--verification of the output: awk -F ',' '{print $2}' 000000_0 | uniq 

--using joins and set operations.
--nested sub query

select * from orders limit 10;
--select * from (nested sub query) limit 10; we have to provide alias for the nested subquery to work in hive.
--output of subquery is stored into q which is further used in the from clause for outer query.
select * from (select * from orders)q limit 10; 


/* 
1.create and load the data into wordcount table
2.select split(s,' ') from wordcount; returns array of strings for the words within each line
3.select explode((split(s,' '))) from wordcount; returns individual words from array of strings
4.As we are unable to give explode on group by clause we have to take nested query approach for grouping data and generating counts
5.using with clause(see more examples on it) we can define a variable(q) globally for results of nested query and it can be used to perform any operations like grouping in 
main query.  
*/ 
with q as 
(select explode(split(s, ' ')) as word from wordcount)
select word,count(1) from q
group by word;

--select is a keyword in hql 
--get all the orders that are in orders but not in order_items using NOT and IN operator
--we have to write sub query to get order_item_order_id from order_items then use it in where clause 
--of the main query.why alias is not used here? is it only in cases like we have to use in from 
--we can use joins instead of these operators for better performance.  
select * from orders
where order_id not in (select order_item_order_id from order_items)
limit 10;

--get all the orders that are in order_items.
select * from orders
where order_id in (select order_item_order_id from order_items)
limit 10;

--with exists we have to use correlated sub query as we are like doing join on orders orderid with order_items order_item_order_id
--we are checking all rows from order_items where oi.order_items_order_id = o.order_id then selecting orders that exists in order_items.
select * from orders 
where exists (
 select 1 from order_items
 where order_items.order_item_order_id = orders.order_id)
limit 10;

--we are checking all rows from order_items where oi.order_items_order_id = o.order_id then selecting orders that don't exist in order_items.
select * from orders o
where not exists(
    select 1 from order_items
    where order_item_order_id = o.order_id
) limit 10;

--joins 
--in transactional databases we will be using normalized data model where data will be stored in many tables, 
--if it is data warehouse applications then we will be storing data in star and snowflake schema where data will be denormalized into fewer tables 
--to serve the reports.earlier we have seen to check if data exists or not exists in other table based on one column but if we 
--want to get data from both tables to serve some report then we have to join both the datasets then only we can get data for both datasets.   
--legacy syntax vs ascii join syntax(below), to collab(combine) data from multiple tables we will be using joins.
select t1.*,t2.* from table t1 [OUTER] JOIN table t2
on t1.id = t2.id
where filters;

--inner join 
--orders and order_items both are parent and child tables.order_id is the primary key and order_item_order_id is the foriegn key
--In hive we can't enforce these constraints but we can do it in source db
--the relation between orders and order_items is 1 to many meaning each and every record in order_items for order_item_order_id  
--has corresponding entry for orderid in orders and no record only in order_items but not in orders
--as order_items had 3 records with order_id 2 it returned all the three records.
select o.order_id,o.order_date,o.order_status,
    oi.order_item_product_id,oi.order_item_subtotal
from orders o INNER JOIN order_items oi
on o.order_id = oi.order_item_order_id
limit 10;

--returns all the mathcing records for a key in child table(172198)
select count(1) from 
orders o INNER JOIN order_items oi
on o.order_id = oi.order_item_order_id;

--query to join multiple tables in hive, here t2 is child table for t1 and t3 tables.
SELECT * FROM 
TABLE1 T1 join table t2
on t1.key = t2.related_key
join table3 on t3.key = t2.related_t3_key 

--count will be less than total records in child table as we are applying the filter condition.
--here we are trying to get all the orders with complete and closed status only 
-- In retail_db depts,categories,products if we want to get revenue per department or products we have to join orders,order_items,products,categories and departments.
select o.order_id,o.order_date,o.order_status,
    oi.order_item_product_id,oi.order_item_subtotal
from orders o JOIN order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status IN ('COMPLETE','CLOSED')
limit 10;

--here the count will be less than 172198 which is 75408 as we are only getting complete and closed orders.
select count(1)
from orders o JOIN order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status IN ('COMPLETE','CLOSED');

--68883
select count(distinct order_id) from orders;

--there are some orders for which order_items doesn't exist so count will be less than 68883 as inner join will skip those records(57431)
-- there are only 57431 unique orders which has corresponding order_items.
select count(distinct o.order_id)
from orders o join order_items oi
on o.order_id = oi.order_item_order_id;


--we have to join both the datasets and if we don't have any entry like order_items for a given 
--orderid then we should get that data as well.

--for left outer join we need to see all records present in order_items for a given order id and 
--if no records are present then we are given with null values.

--based on column in left table we are getting all the records in right table and if any of the doesn't exist 
--then replace with nulls.

--parent table orders doesn't have any duplicates.

select o.order_id,o.order_date,o.order_status,
    oi.order_item_product_id,oi.order_item_subtotal
from orders o left outer JOIN order_items oi
on o.order_id = oi.order_item_order_id
limit 10;

--68883
--it returns all the rows in orders.(183650)
select count(distinct o.order_id)
from orders o left outer JOIN order_items oi
on o.order_id = oi.order_item_order_id;

--order_item_id is primary key means it is not null and also unique 
select o.*
from orders o left outer JOIN order_items oi
on o.order_id = oi.order_item_order_id
where oi.order_item_id is null 
limit 10;

--no of orders which doesn't have corresponding order_items is 11452
--i.e.,total elements which are present in one dataset but not in other
select count(1)
from orders o left outer JOIN order_items oi
on o.order_id = oi.order_item_order_id
where oi.order_item_id is null; 

--for right outer join this query also results in 11452 rows
select count(1)
from order_items oi right outer join orders o
on o.order_id = oi.order_item_order_id
where oi.order_item_id is null;

--no record only in order_items but not in orders so count is 0.
select count(1)
from orders o right outer join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_id is null;

--full outer join same as left outer join union right outer join
--there is no such record where orders entries are null and only order_item are available
select o.order_id,o.order_date,o.order_status,
  oi.order_item_product_id, oi.order_item_subtotal
from orders o full outer join order_items oi
on o.order_id = oi.order_item_order_id
limit 100;   

--default join is reduce side join 
select o.order_id,o.order_date,o.order_status,
    oi.order_item_product_id,oi.order_item_subtotal
from orders o JOIN order_items oi
on o.order_id = oi.order_item_order_id
limit 10;

set hive.auto.convert.join=true;
hive.mapjoin.smalltable.filesize = 25000000; 250MB

--using the legacy approach 
select o.order_id,o.order_date,o.order_status,
    oi.order_item_product_id,oi.order_item_subtotal
from orders o, order_items oi
where o.order_id = oi.order_item_order_id;

--cartesian 
--for each record in order there will be 172
select o.order_id,o.order_date,o.order_status,
    oi.order_item_product_id,oi.order_item_subtotal
from orders o cross join order_items oi
limit 10;


--union is supported but intersection and minus are not supported
create table orders_2013_08_to_2013_11
row format delimited fields terminated by ','
as 
select * from orders 
where order_date >= '2013-08-01 00:00:00.0' and 
      order_date <= '2013-11-30 00:00:00.0'; 

select order_date,count(1) from orders_2013_08_to_2013_11
group by order_date;

--GETTING count of all dates based on months
select date_format(order_date,'YYYYMM'),COUNT(1)
from orders_2013_08_to_2013_11
group by date_format(order_date,'YYYYMM');

--validating that counts are matching
select date_format(order_date,'YYYYMM'),COUNT(1)
from orders
group by date_format(order_date,'YYYYMM');

create table orders_2013_09_to_2013_12
row format delimited fields terminated by ','
as
select * from orders 
where order_date >= '2013-09-01 00:00:00.0' and
      order_date <= '2013-12-31 00:00:00.0';

select date_format(order_date,'YYYYMM'),COUNT(1)
from orders_2013_09_to_2013_12
group by date_format(order_date,'YYYYMM');

--union we need to have same datatypes between the columns 
--union distinct removes duplicate records before performing union operation.
select c1,c2,c3 from table1
union 
select e1,e2,e3 from table2
union 
select f1,f2,f3 from table3;

select count(1) from orders_2013_08_to_2013_11
union all 
select count(1) from orders_2013_09_to_2013_12;

select 'orders_2013_08_to_2013_11',count(1) from orders_2013_08_to_2013_11
union 
select 'orders_2013_09_to_2013_12',count(1) from orders_2013_09_to_2013_12;

select order_id,order_date,order_status from orders_2013_08_to_2013_11
union 
select order_id,order_date,order_status from orders_2013_09_to_2013_12;

select * from (
select order_id,order_date,order_status from orders_2013_08_to_2013_11
union 
select order_id,order_date,order_status from orders_2013_09_to_2013_12    
) q limit 10;

select count(1) from (
select order_id,order_date,order_status from orders_2013_08_to_2013_11
union all
select order_id,order_date,order_status from orders_2013_09_to_2013_12    
) q;

--there is no out of box support for intersection and minus 




























