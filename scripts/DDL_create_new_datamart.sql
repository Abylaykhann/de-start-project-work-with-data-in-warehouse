DROP TABLE IF EXISTS dwh.load_dates_customer_report_datamart;

CREATE TABLE IF NOT EXISTS dwh.load_dates_customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    load_dttm DATE NOT NULL,
    CONSTRAINT load_dates_customer_report_datamart_pk PRIMARY KEY (id));



drop table if exists dwh.customer_report_datamart;

CREATE TABLE dwh.customer_report_datamart (
	id int8 GENERATED ALWAYS AS IDENTITY NOT NULL,
	customer_id int8 NOT NULL,
	customer_name varchar NOT NULL,
	customer_address varchar NOT NULL,
	customer_birthday date NOT NULL,
	customer_email varchar NOT NULL,
	customer_money numeric(15, 2) NOT NULL,
	platform_money int8 NOT NULL,
	count_order int8 NOT NULL,
	avg_price_order numeric(10, 2) NOT NULL,
	median_time_order_completed numeric(10, 1) NULL,
	top_product_category varchar NOT NULL,				--+
	top_craftsman_for_customer int8 not null,			--+
	count_order_created int8 NOT NULL,
	count_order_in_progress int8 NOT NULL,
	count_order_delivery int8 NOT NULL,
	count_order_done int8 NOT NULL,
	count_order_not_done int8 NOT NULL,
	report_period varchar NOT NULL,
	CONSTRAINT customer_report_datamart_pk PRIMARY KEY (id));
