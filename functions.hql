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