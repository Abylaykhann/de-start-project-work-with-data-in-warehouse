drop table if exists dwh.load_dates_customer_report_datamart;

CREATE TABLE if not exists dwh.load_dates_customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    load_dttm DATE NOT NULL,
    CONSTRAINT load_dates_customer_report_datamart_pk PRIMARY KEY (id));



drop table if exists dwh.customer_report_datamart;

CREATE TABLE dwh.customer_report_datamart (
	id int GENERATED ALWAYS AS IDENTITY NOT NULL,
	customer_id int NOT NULL,
	customer_name varchar(100) NOT NULL,
	customer_address varchar(100) NOT NULL,
	customer_birthday date NOT NULL,
	customer_email varchar NOT NULL,
	customer_money numeric(15, 2) NOT NULL,
	platform_money int NOT NULL,
	count_order int NOT NULL,
	avg_price_order numeric(10, 2) NOT NULL,
	median_time_order_completed numeric(10, 1) NULL,
	top_product_category varchar(10) NOT NULL,
	top_craftsman_for_customer int not null,
	count_order_created int NOT NULL,
	count_order_in_progress int NOT NULL,
	count_order_delivery int NOT NULL,
	count_order_done int NOT NULL,
	count_order_not_done int NOT NULL,
	report_period varchar(100) NOT NULL,
	CONSTRAINT customer_report_datamart_pk PRIMARY KEY (id));

