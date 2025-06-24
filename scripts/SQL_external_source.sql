begin transaction

CREATE TEMP TABLE tmp_source_4 as
select
	order_id,
	order_created_date,
	order_completion_date,
	order_status,
	craftsman_id,
	craftsman_name,
	craftsman_address,
	craftsman_birthday,
	craftsman_email,
	product_id,
	product_name,
	product_description,
	product_type,
	product_price,
	o.customer_id,
	c.customer_name,
	c.customer_address,
	c.customer_birthday,
	c.customer_email
FROM external_source.craft_products_orders AS o
	join external_source.customers c 
	on o.customer_id = c.customer_id
	
UNION

select 	order_id,
		order_created_date,
		order_completion_date,
		order_status,
		craftsman_id,
		craftsman_name,
		craftsman_address,
		craftsman_birthday,
		craftsman_email,
		product_id,
		product_name,
		product_description,
		product_type,
		product_price,
       c.customer_id,
	   c.customer_name,
	   c.customer_address,
	   c.customer_birthday,
	   c.customer_email
from external_source.customers c 
	join external_source.craft_products_orders cpo 
	on c.customer_id = cpo.customer_id;

MERGE INTO dwh.d_craftsman d
USING (select distinct 
			craftsman_name,
			craftsman_address,
			craftsman_birthday,
			craftsman_email 
       FROM tmp_source_4) t
ON d.craftsman_name = t.craftsman_name 
AND d.craftsman_email = t.craftsman_email
WHEN MATCHED THEN
  UPDATE SET 
  	craftsman_address = t.craftsman_address, 
	craftsman_birthday = t.craftsman_birthday, 
	load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm)
  VALUES (t.craftsman_name, t.craftsman_address, t.craftsman_birthday, t.craftsman_email, current_timestamp);

/* обновление существующих записей и добавление новых в dwh.d_products */
merge into dwh.d_product d
using (
select
	distinct product_name,
	product_description,
	product_type,
	product_price
from tmp_source_4) t
on
	d.product_name = t.product_name
	and d.product_description = t.product_description
	and d.product_price = t.product_price
when matched then
  update set
	product_type = t.product_type,
	load_dttm = current_timestamp
when not matched then
  insert
	(product_name,
	product_description,
	product_type,
	product_price,
	load_dttm)
  values (t.product_name,
	t.product_description,
	t.product_type,
	t.product_price,
	current_timestamp);

/* обновление существующих записей и добавление новых в dwh.d_customer */
merge into dwh.d_customer d
using (
select
	distinct customer_name,
	customer_address,
	customer_birthday,
	customer_email
from tmp_source_4) t
on
	d.customer_name = t.customer_name
	and d.customer_email = t.customer_email
when matched then
  update set
	customer_address = t.customer_address,
	customer_birthday = t.customer_birthday,
	load_dttm = current_timestamp
when not matched then
  insert
	(customer_name,
	customer_address,
	customer_birthday,
	customer_email,
	load_dttm)
  values (t.customer_name,
		t.customer_address,
		t.customer_birthday,
		t.customer_email,
		current_timestamp);

CREATE temp TABLE tmp_source_4_fact AS 
SELECT  dp.product_id,
        dc.craftsman_id,
        dcust.customer_id,
        src.order_created_date,
        src.order_completion_date,
        src.order_status,
        current_timestamp 
FROM tmp_source_4 src
JOIN dwh.d_craftsman dc ON dc.craftsman_name = src.craftsman_name and dc.craftsman_email = src.craftsman_email 
JOIN dwh.d_customer dcust ON dcust.customer_name = src.customer_name and dcust.customer_email = src.customer_email 
JOIN dwh.d_product dp ON dp.product_name = src.product_name and dp.product_description = src.product_description and dp.product_price = src.product_price;

/* обновление существующих записей и добавление новых в dwh.f_order */
merge into dwh.f_order f
using tmp_source_4_fact t
on
	f.product_id = t.product_id
	and f.craftsman_id = t.craftsman_id
	and f.customer_id = t.customer_id
	and f.order_created_date = t.order_created_date
when matched then
  update set
	order_completion_date = t.order_completion_date,
	order_status = t.order_status,
	load_dttm = current_timestamp
when not matched then
  insert
	(product_id,
	craftsman_id,
	customer_id,
	order_created_date,
	order_completion_date,
	order_status,
	load_dttm)
  values (t.product_id,
	t.craftsman_id,
	t.customer_id,
	t.order_created_date,
	t.order_completion_date,
	t.order_status,
	current_timestamp);

commit transaction