--ssh itv000613@g01.itversity.com u4onzoojuje9rf88fwo4dlfz6vinhe0r

hive --database gouthamkumar_retail
use gouthamkumar_retail;

create table orders(
order_id INT,
order_date STRING,
order_customer_id INT,
order_status STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

--gives the metadata about the table.
describe formatted orders;

--with hive it is always schema on read vs the traditional rdbms as schema on write if we try to delete any record in the 
--parent table then it will throw an error saying it is linked with child table.
--when we try to insert, update and delete a record then schema mismatch error is thrown while inserting but hive throws only 
--such exceptions while reading the data.

--creating table with column and table level comments.
CREATE TABLE orders (
  order_id INT COMMENT 'Unique order id',
  order_date STRING COMMENT 'Date on which order is placed',
  order_customer_id INT COMMENT 'Customer id who placed the order',
  order_status STRING COMMENT 'Current status of the order'
) COMMENT 'Table to save order level details'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

--shows comments of the table.
describe formatted orders;

-- the default delimiter in hive is ASCII01 or ctrlA character so when we load a ',' delimited file then we will 
-- get error so we have to create table with fields terminated by ','.

--used to load data from local file system.
LOAD DATA LOCAL INPATH '/data/retail_db/orders' into table orders;

--used to delete the data from table.
truncate table orders;

--list the files in the hdfs directory
dfs -ls hdfs://m01.itversity.com:9000/user/itv000613/db/orders;
--view last 100 records in the hdfs directory
dfs -tail hdfs://m01.itversity.com:9000/user/itv000613/db/orders/part-00000;

--get the count of total records in table--137766  
select count(1) from orders;













create table order_items(
	order_item_id INT,
	order_item_order_id INT,
	order_item_product_id INT,
	order_item_quantity INT,
	order_item_subtotal FLOAT,
	order_item_product_price FLOAT
) STORED AS ORC;

create table order_items_stage(
	order_item_id INT,
	order_item_order_id INT,
	order_item_product_id INT,
	order_item_quantity INT,
	order_item_subtotal FLOAT,
	order_item_product_price FLOAT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

load data local inpath '/data/retail_db/order_items' into table order_items_stage;
select * from order_items_stage limit 10;
select count(1) from order_items_stage;

--we are using order_items_stage so that ',' delimited file order_items is loaded into default delimited file.
insert into table order_items select * from order_items_stage;
dfs -ls hdfs://m01.itversity.com:9000/user/itv000613/db/order_items;
dfs -tail hdfs://m01.itversity.com:9000/user/itv000613/db/order_items/000000_0;

truncate table order_items;
insert overwrite table order_items select * from order_items_stage;

create table orders_part(order_id INT,order_date STRING,order_customer_id INT,order_status STRING) PARTITIONED BY (order_month INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

describe formatted orders_part;
load data local inpath '/data/retail_db/orders' into table orders_part;

alter table orders_part add partition(order_month=201307);

alter table orders_part add 
PARTITION(order_month=201308)
PARTITION(order_month=201309)
PARTITION(order_month=201310);

--get uniq dates in a file and counting them 
awk -F "," '{print $2}' part-00000|sort|uniq|wc -l 
--getting 1 month data from file

grep 2013-07 part-00000 > ~/orders/orders_201307
grep 2013-08 part-00000 > ~/orders/orders_201308
grep 2013-09 part-00000 > ~/orders/orders_201309
grep 2013-10 part-00000 > ~/orders/orders_201310

load data local inpath '/home/itv000613/orders/orders_201307' into table orders_part partition(order_month=201307);
load data local inpath '/home/itv000613/orders/orders_201308' into table orders_part partition(order_month=201308);
load data local inpath '/home/itv000613/orders/orders_201309' into table orders_part partition(order_month=201309);
load data local inpath '/home/itv000613/orders/orders_201310' into table orders_part partition(order_month=201310);

select order_month,count(1) from orders_part
group by order_month;

alter table orders_part add PARTITION(order_month=201311);
select count(1) from orders where order_date like '2013-11%';

insert into table orders_part partition (order_month=201311) select * from orders where order_date like '2013-11%';

select date_format(order_date,'YYYYMM'),COUNT(1)
from orders
group by date_format(order_date,'YYYYMM');

--dynamic partitions
hive -e "SET;"| grep dynamic.partition
set hive.exec.dynamic.partition.mode=nonstrict;

insert into table orders_part partition(order_month)
select o.*,date_format(order_date,'YYYYMM') as order_month
from orders o
where o.order_date >= "2013-12-01 00:00:00.0";

--bucketing 
--whenever we divide the order_id with 8 remainder will be zero then it will go to first part file, 
--remainder 1 will go to next part file etc.,
create table orders_buck(
order_id INT,
order_date STRING,
order_customer_id INT,
order_status STRING
) 
CLUSTERED BY (order_id) into 8 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

dfs -ls hdfs://m01.itversity.com:9000/user/itv000613/db/orders_buck;
hive -e "SET;" | grep -i bucket  
insert into table orders_buck select * from orders;

describe formatted orders_buck;

--whenever we give sort by the data will be sorted within the buckets.
create table orders_buck(
order_id INT,
order_date STRING,
order_customer_id INT,
order_status STRING
) 
CLUSTERED BY (order_id)
SORTED by (order_id) into 8 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

insert into table orders_buck select * from orders;

--for creating acid table it should be bucketed first and stored as orc file format. 
drop table orders;
CREATE TABLE orders (
  order_id STRING COMMENT 'Unique order id',
  order_date STRING COMMENT 'Date on which order is placed',
  order_customer_id INT COMMENT 'Customer id who placed the order',
  order_status STRING COMMENT 'Current status of the order'
) COMMENT 'Table to save order level details'
CLUSTERED BY (order_id) into 8 buckets 
stored as orc;

-- we have to insert batch records not single record because it will take the same time to insert. 
INSERT into orders values(1,'2013-07-25 00:00:00.0',1000,'COMPLETE');
alter table orders set tblproperties('transactional'='true');

SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
UPDATE ORDERS SET ORDER_STATUS = 'PENDING' WHERE order_id = 1;

CREATE TABLE orders (
  order_id STRING COMMENT 'Unique order id',
  order_date STRING COMMENT 'Date on which order is placed',
  order_customer_id INT COMMENT 'Customer id who placed the order',
  order_status STRING COMMENT 'Current status of the order'
) COMMENT 'Table to save order level details'
CLUSTERED BY (order_id) 
into 8 buckets 
stored as orc
tblproperties('transactional'='true');

--compaction will run to combine small delta files into larger files 
insert into orders values (1,'2013-07-25 00:00:00.0',1000,'COMPLETE');
insert into orders values (2,'2013-07-25 00:00:00.0',2100,'COMPLETE');
insert into orders values 
(3,'2013-07-25 00:00:00.0',1500,'PENDING'),
(4,'2013-07-25 00:00:00.0',1200,'COMPLETE'),
(5,'2013-07-25 00:00:00.0',1400,'CLOSED');

update orders set order_status = 'complete' where order_status = 'COMPLETE';
truncate table orders;
insert into orders select order_id,order_date,order_customer_id,order_status from orders_part;
dfs -ls hdfs://m01.itversity.com:9000/user/itv000613/db/orders/delta_0000008_0000008_0000;

--Functions
show functions;
describe function substr;
describe function substring;

select current_date;
select substr('Hello World',1,5);

use gouthamkumar_retail;

create table dual(dummy STRING);
INSERT into table dual values('X');
select substr('Hello World',1,5) from dual;

select ('heLLo World');
select lower('hello WORLD');
select upper('hello world');
select initcap('hello world');
select length('hello world');

select order_id,order_date,order_customer_id,upper(order_status) as order_status from orders limit 10;

SELECT substr('2013-07-25 00:00:00.0', 1, 4);
SELECT substr('2013-07-25 00:00:00.0', 6, 2);
SELECT substr('2013-07-25 00:00:00.0', 9, 2);
SELECT substr('2013-07-25 00:00:00.0', 12);

SELECT order_id,
  substr(order_date, 1, 10) AS order_date,
  order_customer_id,
  order_status
FROM orders_part LIMIT 10;

SELECT split('2013-07-25', '-')[1];
SELECT explode(split('2013-07-25', '-'));

describe function ltrim;
describe function trim;
select '     Hello World';
select ltrim('     Hello World');
select rtrim('     Hello World   ');
select length(trim('     Hello World   '));
select 2013 as year,7 as month, 25 as myDate from dual;
describe function lpad;
select lpad(7,2,0);
select lpad(10,2,0);
select lpad(100,2,0);

describe function reverse;
select reverse('Hello World');
describe function concat;
select concat ('Hello ','World');
select concat('Order Status is ', order_status) FROM orders LIMIT 10;

select concat(year,'-',lpad(month,2,0),'-',lpad(day,2,0)) as order_date
from 
(select 2013 as year,7 as month, 25 as day from dual)q;

select current_date;
select current_timestamp;

SELECT date_add(current_date, 32);
SELECT date_add('2018-04-15', 730);
SELECT date_add('2018-04-15', -730);
SELECT date_sub(current_date, 30);

SELECT datediff('2019-03-30', '2017-12-31');

--if the date is not available it will give last date of the coming month 
select add_months('2019-01-31',1);
select add_months('2019-05-31',1);
describe function add_months;
select add_months(current_timestamp,1);
select date_add(current_timestamp,-730);

describe function trunc;
select trunc(current_date,'MM');
select trunc('2019-01-23','MM');
SELECT TRUNC(CURRENT_DATE,'YY');
SELECT TRUNC(CURRENT_TIMESTAMP,'HH');

describe function date_format;
select current_timestamp,date_format(current_timestamp,'YYYY');
select current_timestamp,date_format(current_timestamp,'YY');
select current_timestamp,date_format(current_timestamp,'Y');

select current_timestamp,date_format(current_timestamp,'MM');
select current_timestamp,date_format(current_timestamp,'dd');
--It will give the number of days from starting year.
select current_timestamp,date_format(current_timestamp,'DD');

select current_timestamp,date_format(current_timestamp,'HH');
--It will give hours 
select current_timestamp,date_format(current_timestamp,'hh');
select current_timestamp,date_format(current_timestamp,'mm');
select current_timestamp,date_format(current_timestamp,'ss');
--It will extract time in milli seconds 
select current_timestamp,date_format(current_timestamp,'SS');
select date_format(current_timestamp,'YYYYMM');
select date_format(current_timestamp,'YYYYMMdd');
select date_format(current_timestamp,'YYYY/MM/dd');

describe function day;
DESCRIBE FUNCTION dayofmonth;
DESCRIBE FUNCTION month;
DESCRIBE FUNCTION weekofyear;
DESCRIBE FUNCTION year;

select year(current_date);
SELECT month(current_date);
SELECT weekofyear(current_date);
SELECT day(current_date);
--it gives day of month in the date part.
SELECT dayofmonth(current_date);

-- run this shell command to get epoc -> date '+%s' minutes that are passed since January 1st 1970
--converts epoch to unixtime
SELECT from_unixtime(1556662731);
SELECT to_unix_timestamp('2019-04-30 18:18:51');

SELECT from_unixtime(1556662731, 'YYYYMM');
SELECT from_unixtime(1556662731, 'YYYY-MM-dd');
SELECT from_unixtime(1556662731, 'YYYY-MM-dd HH:mm:ss');

SELECT to_unix_timestamp('20190430 18:18:51', 'YYYYMMdd');
SELECT to_unix_timestamp('20190430 18:18:51', 'YYYYMMdd HH:mm:ss');

--mathematical functions 
describe function abs;
select abs(-10);
SELECT avg(order_item_subtotal) FROM order_items WHERE order_item_order_id = 2;
SELECT round(193.33, 1);
--returns before integer in the decimal part.
select ceil(193.33);
select floor(193.33);
select pow(2,3);
select sqrt(4);
--returns number between 0 and 1.
select rand();

describe function split;
select split(current_date,'-')[1];

select cast(split(current_date,'-')[1] as INT);
SELECT CONCAT(0,4);
select cast('0.04' as FLOAT);
select cast('0.04' as int);

--handling null values using nvl function
select 1+NULL;
DESCRIBE function nvl;
select nvl(1,0);
select nvl(NULL,0);

/*1.create and load the data into wordcount table
2.select split(s,' ') from wordcount; returns array of strings for the words within each line
3.select explode((split(s,' '))) from wordcount; returns individual words from array of strings
4.As we are unable to give explode on group by clause we have to take nested query approach for grouping data and generating counts
*/ 
create table wordcount(s STRING);
INSERT INTO wordcount VALUES
  ('Hello World'),
  ('How are you'),
  ('Let us perform the word count'),
  ('The definition of word count is'),
  ('to get the count of each word from this data');

select word,count(1)
from 
 (select explode(split(s,' ')) as word from wordcount)q
 group by word;