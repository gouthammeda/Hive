--ssh itv000613@g01.itversity.com u4onzoojuje9rf88fwo4dlfz6vinhe0r
use gouthamkumar_retail;
hive --database gouthamkumar_retail

--writing basic queries:

select order_item_order_id,sum(order_item_subtotal) as order_revenue
  from order_items
  group by order_item_order_id
  limit 10;

select order_id,order_customer_id from orders limit 10;
select order_item_order_id,order_item_product_id,
    order_item_quantity * order_item_product_price as order_item_revenue
     from order_items
     limit 10;

select o.*,
 CASE WHEN o.order_status IN('COMPLETE','CLOSED') THEN 'COMPLETED'
      WHEN o.order_status IN('PENDING','PENDING_PAYMENT','PAYMENT_REVIEW','PROCESSING') THEN 'PENDING'
      ELSE 'OTHER'
 END AS actual_status
from orders o limit 10;

select distinct order_date from orders;
select distinct order_status from orders;
select distinct order_item_product_id from order_items;
select count(distinct order_date) from orders;
--come back
select distinct * from orders limit 10;
select * from orders limit 10;
--always run distinct when it is really needed.


select * from orders where order_status='CLOSED' limit 10;
select * from orders where order_status='COMPLETE' limit 10;
SELECT * FROM ORDERS WHERE order_customer_id = 8827;
select * from order_items where order_item_quantity >= 2 limit 10;
select * from order_items where order_item_subtotal >= 100 limit 10;
select * from orders where order_status !='COMPLETE' limit 10;
select * from orders where order_status <> 'COMPLETE' limit 10; 

select * from orders where order_status='COMPLETE' and order_date = '2013-07-25 00:00:00.0' LIMIT 10;
select COUNT(1) from orders where order_status='COMPLETE' and order_date = '2013-07-25 00:00:00.0';
select * from orders where order_status='COMPLETE' OR order_date = '2013-07-25 00:00:00.0' LIMIT 10;
select count(1) from orders where order_status='COMPLETE' OR order_date = '2013-07-25 00:00:00.0';
select count(1) from orders where order_status='COMPLETE';
SELECT COUNT(1) FROM ORDERS where order_status <> 'COMPLETE' and order_date = '2013-07-25 00:00:00.0';

select * from orders 
 where order_status = 'COMPLETE' or order_status='CLOSED'
 LIMIT 10;

select * from orders 
 where order_status in('COMPLETE','CLOSED','PENDING')
 LIMIT 100;

SELECT * FROM ORDERS 
 WHERE ORDER_STATUS <> 'COMPLETE' AND ORDER_STATUS <> 'CLOSED'
 LIMIT 10;

SELECT ORDER_DATE,COUNT(1) FROM ORDERS 
  WHERE ORDER_DATE LIKE '%-07-%'
  GROUP BY ORDER_DATE;

select sum(order_item_subtotal) as order_revenue,
       min(order_item_subtotal) as min_order_item_subtotal,
       max(order_item_subtotal) as max_order_item_subtotal,
       avg(order_item_subtotal) as avg_order_item_subtotal
from order_items
where order_item_order_id = 2;

select order_item_order_id,
       sum(order_item_subtotal) order_revenue,
       min(order_item_subtotal) min_order_item_subtotal,
       max(order_item_subtotal) max_order_item_subtotal,
       avg(order_item_subtotal) avg_order_item_subtotal,
       count(order_item_subtotal) cnt_order_item_subtotal
from order_items
group by order_item_order_id
limit 10;

select order_date,count(distinct order_status) distinct_order_status_count
from orders
group by order_date
limit 10;

--shouldn't use the below approach
select order_date,count(distinct order_status) distinct_order_status_count
from orders
group by order_date,order_status
limit 10;

select order_item_order_id,sum(order_item_subtotal) as order_revenue
from order_items
group by order_item_order_id
having sum(order_item_subtotal)>=500
limit 10;

select * from orders order by order_customer_id limit 10;
select * from orders 
order by order_customer_id,order_date
limit 10;

select * from orders 
order by order_customer_id asc, order_date desc
limit 10;

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

insert into stocks_eod_orderby
select * from stocks_eod
order by tradedate,volume desc;

create table stocks_eod_sortby (
 stockticker STRING,
 tradedate int,
 openprice float,
 highprice float,
 lowprice float,
 closeprice float,
 volume bigint
 ) row format delimited fields terminated by ',';

insert into table stocks_eod_sortby
select * from stocks_eod
distribute by tradedate sort by tradedate,volume desc;
 
describe formatted stocks_eod_sortby;
if we want to use sort within a key then distribute by and sort by have to be used instead of order by.

--come back for 0 records.
create table stocks_eod_clusterby
row format delimited fields terminated by ','
as 
select * from stocks_eod
cluster by tradedate; 

--using joins and set operations.

with q as 
(select explode(split(s, ' ')) as word from wordcount)
select word,count(1) from q
group by word;

select * from orders
where order_id not in (select order_item_order_id from order_items)
limit 10;

select * from orders
where order_id in (select order_item_order_id from order_items)
limit 10;

select * from orders 
where exists (
 select 1 from order_items
  where order_items.order_item_order_id = orders.order_id)
limit 10;

select t1.*,t2.* from table t1 [OUTER] JOIN table t2
on t1.id = t2.id
where filters;

-- the relation between orders and order_items is 1 to many meaning each and 
--every record in order item has corresponding entry in orders and no record only in order_items but not in orders
select o.order_id,o.order_date,o.order_status,
    oi.order_item_product_id,oi.order_item_subtotal
from orders o INNER JOIN order_items oi
on o.order_id = oi.order_item_order_id
limit 10;

--returns all the mathcing records for a key in child table 

select count(1) from 
orders o INNER JOIN order_items oi
on o.order_id = oi.order_item_order_id;

SELECT * FROM 
TABLE1 T1 join table t2
on t1.key = t2.related_key
join table3 on t3.key = t2.related_t3_key 

--count will be less than total records in child table as we are applying the filter condition.
select o.order_id,o.order_date,o.order_status,
    oi.order_item_product_id,oi.order_item_subtotal
from orders o INNER JOIN order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in('COMPLETE','CLOSED')
limit 10;

select count(distinct o.order_id)
from orders o join order_items oi
on o.order_id = oi.order_item_order_id;

-- for left outer join we need to see all records present in order_items for a given order id and 
--if no records are present then we have to replace them with nulls.
select o.order_id,o.order_date,o.order_status,
    oi.order_item_product_id,oi.order_item_subtotal
from orders o left outer JOIN order_items oi
on o.order_id = oi.order_item_order_id;

--it returns all the rows in orders.
select count(1)
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
select order_id,order_date,order_status from orders_2013_09_to_2013_12
;

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

there is no out of box support for intersection and minus 



























