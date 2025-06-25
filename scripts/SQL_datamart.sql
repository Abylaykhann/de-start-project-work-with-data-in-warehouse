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




with 
dwh_delta AS ( -- определяем, какие данные были изменены в витрине или добавлены в DWH. Формируем дельту изменений
SELECT     
            dcs.customer_id AS customer_id,
            dcs.customer_name AS customer_name,
            dcs.customer_address AS customer_address,
            dcs.customer_birthday AS customer_birthday,
            dcs.customer_email AS customer_email,
            fo.order_id AS order_id,
            dp.product_id AS product_id,
            dp.product_price AS product_price,
            dp.product_type AS product_type,
            fo.order_completion_date - fo.order_created_date AS diff_order_date, 
            fo.order_status AS order_status,
            TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
            crd.customer_id AS exist_customer_id,
            dc.load_dttm AS craftsman_load_dttm,
            dcs.load_dttm AS customers_load_dttm,
            dp.load_dttm AS products_load_dttm,
            dc.craftsman_id as craftsman_id
            FROM dwh.f_order fo 
                INNER JOIN dwh.d_craftsman dc ON fo.craftsman_id = dc.craftsman_id 
                INNER JOIN dwh.d_customer dcs ON fo.customer_id = dcs.customer_id 
                INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id 
                LEFT JOIN dwh.customer_report_datamart crd ON dcs.customer_id = crd.customer_id
            WHERE (fo.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
                  (dc.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
                  (dcs.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
                  (dp.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart))
),
dwh_update_delta AS ( -- делаем выборку ЗАКАЗЧИКОВ, по которым были изменения в DWH. По этим ЗАКАЗЧИКАМ данные в витрине нужно будет обновить
    SELECT dd.exist_customer_id AS customer_id
    FROM dwh_delta dd 
    WHERE dd.exist_customer_id IS NOT NULL        
),
dwh_delta_insert_result AS ( -- делаем расчёт витрины по новым данным. 
							 -- Этой информации по заказчикам в рамках расчётного периода раньше не было, это новые данные. 
							 -- Их можно просто вставить (insert) в витрину без обновления
    SELECT  
            T4.customer_id_main AS customer_id,
            T4.customer_name AS customer_name,
            T4.customer_address AS customer_address,
            T4.customer_birthday AS customer_birthday,
            T4.customer_email AS customer_email,
            T4.customer_money AS customer_money,
            T4.platform_money AS platform_money,
            T4.count_order AS count_order,
            T4.avg_price_order AS avg_price_order,
            T4.median_time_order_completed AS median_time_order_completed,
            T4.product_type AS top_product_category,
            T4.craftsman_id_for_customer as top_craftsman_for_customer,
            T4.count_order_created AS count_order_created,
            T4.count_order_in_progress AS count_order_in_progress,
            T4.count_order_delivery AS count_order_delivery,
            T4.count_order_done AS count_order_done,
            T4.count_order_not_done AS count_order_not_done,
            T4.report_period AS report_period
            FROM (
                SELECT     -- в этой выборке объединяем две внутренние выборки по расчёту столбцов витрины и 
                           -- применяем оконную функцию для определения самой популярной категории товаров
                        *,
                        rank() OVER(PARTITION BY T2.customer_id_main ORDER BY count_product DESC) AS rank_count_product,
                        row_number () over( partition by craftsman_id_for_customer  order by count_craftsman desc ) as rownumber
                        FROM ( 
                            SELECT -- в этой выборке делаем расчёт по большинству столбцов, так как все они требуют одной и той же группировки, 
                                   -- кроме столбца с самой популярной категорией товаров у заказчика. 
                                   -- Для этого столбца сделаем отдельную выборку с другой группировкой и выполним JOIN
                                T1.customer_id AS customer_id_main,
                                T1.customer_name AS customer_name,
                                T1.customer_address AS customer_address,
                                T1.customer_birthday AS customer_birthday,
                                T1.customer_email AS customer_email,
                                SUM(T1.product_price) - (SUM(T1.product_price) * 0.1) AS customer_money,
                                SUM(T1.product_price) * 0.1 AS platform_money,
                                COUNT(order_id) AS count_order,
                                AVG(T1.product_price) AS avg_price_order,
                                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_time_order_completed,
                                SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
                                SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
                                SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
                                SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
                                SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
                                T1.report_period AS report_period
                                FROM dwh_delta AS T1
                                WHERE T1.exist_customer_id IS NULL
                                GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, T1.customer_birthday, T1.customer_email, T1.report_period
                            ) AS T2 
                            INNER JOIN (
                                    SELECT     -- Эта выборка поможет определить самый популярный товар у ЗАКАЗЧИКА ручной работы. 
                                               -- Эта выборка не делается в предыдущем запросе, так как нужна другая группировка. 
                                               -- Для данных этой выборки можно применить оконную функцию, 
                                               -- которая и покажет самую популярную категорию товаров у заказчика
                                            dd.customer_id AS customer_id_for_product_type, 
                                            dd.product_type, 
                                            COUNT(dd.product_id) AS count_product
                                            FROM dwh_delta AS dd
                                            GROUP BY dd.customer_id, dd.product_type
                                            ORDER BY count_product desc
                            ) AS T3 
                            ON T2.customer_id_main = T3.customer_id_for_product_type
                            inner join (
                                    SELECT     -- Эта выборка поможет определить самый популярный товар у ЗАКАЗЧИКА ручной работы. 
                                               -- Эта выборка не делается в предыдущем запросе, так как нужна другая группировка. 
                                               -- Для данных этой выборки можно применить оконную функцию, 
                                               -- которая и покажет самую популярную категорию товаров у заказчика
                                            dd.customer_id AS customer_id, 
                                            dd.craftsman_id as craftsman_id_for_customer, 
                                            COUNT(dd.craftsman_id) AS count_craftsman
                                            FROM dwh_delta AS dd
                                            GROUP BY dd.customer_id, dd.craftsman_id
                                            ORDER BY count_craftsman desc                          
                            ) as t3_2
                            on t2.customer_id_main = t3_2.customer_id		-- _for_craftsman
                ) AS T4 
                WHERE T4.rank_count_product = 1 
                  and t4.rownumber = 1
                ORDER BY report_period -- условие помогает оставить в выборке первую по популярности категорию товаров
),
dwh_delta_update_result AS ( -- делаем перерасчёт для существующих записей витринs, 
							 -- так как данные обновились за отчётные периоды. 
							 -- Логика похожа на insert, но нужно достать конкретные данные из DWH
    SELECT 
            T4.customer_id_main AS customer_id,
            T4.customer_name AS customer_name,
            T4.customer_address AS customer_address,
            T4.customer_birthday AS customer_birthday,
            T4.customer_email AS customer_email,
            T4.customer_money AS customer_money,
            T4.platform_money AS platform_money,
            T4.count_order AS count_order,
            T4.avg_price_order AS avg_price_order,
            T4.median_time_order_completed AS median_time_order_completed,
            T4.product_type AS top_product_category,
            T4.craftsman_id_for_customer as top_craftsman_for_customer,
            T4.count_order_created AS count_order_created,
            T4.count_order_in_progress AS count_order_in_progress,
            T4.count_order_delivery AS count_order_delivery, 
            T4.count_order_done AS count_order_done, 
            T4.count_order_not_done AS count_order_not_done,
            T4.report_period AS report_period 
            FROM (
                SELECT     -- в этой выборке объединяем две внутренние выборки по расчёту столбцов витрины и применяем оконную функцию для определения самой популярной категории товаров
                        *,
                        rank() OVER(PARTITION BY T2.customer_id_main ORDER BY count_product DESC) AS rank_count_product,
                        row_number () over( partition by craftsman_id_for_customer order by count_craftsman desc ) as rownumber                      
                        FROM (
                            SELECT -- в этой выборке делаем расчёт по большинству столбцов, так как все они требуют одной и той же группировки, кроме столбца с самой популярной категорией товаров у мастера. Для этого столбца сделаем отдельную выборку с другой группировкой и выполним JOIN
                                T1.customer_id_main AS customer_id_main,
                                T1.customer_name AS customer_name,
                                T1.customer_address AS customer_address,
                                T1.customer_birthday AS customer_birthday,
                                T1.customer_email AS customer_email,
                                SUM(T1.product_price) - (SUM(T1.product_price) * 0.1) AS customer_money,
                                SUM(T1.product_price) * 0.1 AS platform_money,
                                COUNT(order_id) AS count_order,
                                AVG(T1.product_price) AS avg_price_order,
                                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_time_order_completed,
                                SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created, 
                                SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
                                SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
                                SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
                                SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
                                T1.report_period AS report_period
                                FROM (
                                    SELECT     -- в этой выборке достаём из DWH обновлённые или новые данные по мастерам, которые уже есть в витрине
                                            dcs.customer_id AS customer_id_main,
                                            dcs.customer_name AS customer_name,
                                            dcs.customer_address AS customer_address,
                                            dcs.customer_birthday AS customer_birthday,
                                            dcs.customer_email AS customer_email,
                                            fo.order_id AS order_id,
                                            dp.product_id AS product_id,
                                            dp.product_price AS product_price,
                                            dp.product_type AS product_type,
                                            fo.order_completion_date - fo.order_created_date AS diff_order_date,
                                            fo.order_status AS order_status, 
                                            TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period
                                            FROM dwh.f_order fo 
                                                INNER JOIN dwh.d_craftsman dc ON fo.craftsman_id = dc.craftsman_id 
                                                INNER JOIN dwh.d_customer dcs ON fo.customer_id = dcs.customer_id 
                                                INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id
                                                INNER JOIN dwh_update_delta ud ON fo.customer_id = ud.customer_id
                                ) AS T1
                                GROUP BY T1.customer_id_main, T1.customer_name, T1.customer_address, T1.customer_birthday, T1.customer_email, T1.report_period
                            ) AS T2 
                            INNER JOIN (
                                SELECT     -- Эта выборка поможет определить самый популярный товар у мастера. 
                                           -- Эта выборка не делается в предыдущем запросе, так как нужна другая группировка. 
                                           -- Для данных этой выборки можно применить оконную функцию, которая и покажет 
                                           -- самую популярную категорию товаров у мастера
                                            dd.customer_id AS customer_id_for_product_type, 
                                            dd.product_type, 
                                            COUNT(dd.product_id) AS count_product
                                            FROM dwh_delta AS dd
                                            GROUP BY dd.customer_id, dd.product_type
                                            ORDER BY count_product desc
                            ) AS T3 
                            ON T2.customer_id_main = T3.customer_id_for_product_type       
                            inner join (
                                    SELECT     -- Эта выборка поможет определить самый популярный товар у ЗАКАЗЧИКА ручной работы. 
                                               -- Эта выборка не делается в предыдущем запросе, так как нужна другая группировка. 
                                               -- Для данных этой выборки можно применить оконную функцию, 
                                               -- которая и покажет самую популярную категорию товаров у заказчика
                                            dd.customer_id AS customer_id, 
                                            dd.craftsman_id as craftsman_id_for_customer, 
                                            COUNT(dd.craftsman_id) AS count_craftsman
                                            FROM dwh_delta AS dd
                                            GROUP BY dd.customer_id, dd.craftsman_id
                                            ORDER BY count_craftsman desc                          
                            ) as t3_2
                            on t2.customer_id_main = t3_2.customer_id
                ) AS T4 
                WHERE T4.rank_count_product = 1 
                  and t4.rownumber = 1                
                ORDER BY report_period
),
insert_delta AS (
    INSERT INTO dwh.customer_report_datamart (
        customer_id,
        customer_name,
        customer_address,
        customer_birthday, 
        customer_email, 
        customer_money, 
        platform_money, 
        count_order, 
        avg_price_order, 
        median_time_order_completed,
        top_product_category, 
        top_craftsman_for_customer,
        count_order_created, 
        count_order_in_progress, 
        count_order_delivery, 
        count_order_done, 
        count_order_not_done, 
        report_period
    ) SELECT 
            customer_id,
            customer_name,
            customer_address,
            customer_birthday,
            customer_email,
            customer_money,
            platform_money,
            count_order,
            avg_price_order,
            median_time_order_completed,
            top_product_category,
	        top_craftsman_for_customer,
            count_order_created, 
            count_order_in_progress,
            count_order_delivery, 
            count_order_done, 
            count_order_not_done,
            report_period 
            FROM dwh_delta_insert_result
),
update_delta AS (
    UPDATE dwh.customer_report_datamart SET
        customer_name = updates.customer_name, 
        customer_address = updates.customer_address, 
        customer_birthday = updates.customer_birthday, 
        customer_email = updates.customer_email, 
        customer_money = updates.customer_money, 
        platform_money = updates.platform_money, 
        count_order = updates.count_order, 
        avg_price_order = updates.avg_price_order, 
        median_time_order_completed = updates.median_time_order_completed, 
        top_product_category = updates.top_product_category, 
        top_craftsman_for_customer = updates.top_craftsman_for_customer,
        count_order_created = updates.count_order_created, 
        count_order_in_progress = updates.count_order_in_progress, 
        count_order_delivery = updates.count_order_delivery, 
        count_order_done = updates.count_order_done,
        count_order_not_done = updates.count_order_not_done, 
        report_period = updates.report_period
    FROM (
        SELECT 
            customer_id,
            customer_name,
            customer_address,
            customer_birthday,
            customer_email,
            customer_money,
            platform_money,
            count_order,
            avg_price_order,
            median_time_order_completed,
            top_product_category,
	        top_craftsman_for_customer,
            count_order_created,
            count_order_in_progress,
            count_order_delivery,
            count_order_done,
            count_order_not_done,
            report_period 
            FROM dwh_delta_update_result) AS updates
    WHERE dwh.customer_report_datamart.customer_id = updates.customer_id
),
insert_load_date AS (
    INSERT INTO dwh.load_dates_customer_report_datamart (
        load_dttm)
    SELECT GREATEST(COALESCE(MAX(dd.customers_load_dttm), NOW()), 
                    COALESCE(MAX(dd.craftsman_load_dttm), NOW()), 
                    COALESCE(MAX(dd.products_load_dttm), NOW())) 
        FROM dwh_delta dd)
SELECT 'increment datamart' result;
