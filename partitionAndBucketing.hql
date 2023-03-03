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

--get uniq dates in a file and count them 
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
