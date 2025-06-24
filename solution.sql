-- Этап 1. Создание и заполнение БД

-- 1.1 Сырые данные
DROP SCHEMA IF EXISTS raw_data CASCADE;

CREATE SCHEMA raw_data;

-- Не ограничиваю точность полей, чтобы не потерять никакие данные
CREATE TABLE raw_data.sales(
	id INTEGER,
	auto TEXT,
	gasoline_consumption NUMERIC,
	price NUMERIC,
	sale_date DATE,
	person TEXT,
	phone TEXT,
	discount INTEGER,
	brand_origin TEXT
);

\copy raw_data.sales(id, auto, gasoline_consumption, price, sale_date, person, phone, discount, brand_origin) FROM 'C:\Dev\sales\cars.csv' WITH CSV HEADER NULL 'null'


-- 1.2 Создание целевой схемы

DROP SCHEMA IF EXISTS car_shop CASCADE;

CREATE SCHEMA car_shop;

CREATE TABLE car_shop.country(
	country_id SERIAL NOT NULL PRIMARY KEY,
	country_name VARCHAR NOT NULL UNIQUE
);

CREATE TABLE car_shop.brand(
	brand_id SERIAL NOT NULL PRIMARY KEY,
	brand_name VARCHAR NOT NULL UNIQUE,
	origin_country_id INTEGER REFERENCES car_shop.country(country_id)
);

CREATE TABLE car_shop.model(
	model_id SERIAL NOT NULL PRIMARY KEY,
	model_name VARCHAR NOT NULL,
	brand_id INTEGER NOT NULL REFERENCES car_shop.brand(brand_id),
	is_electric BOOLEAN NOT NULL,
	gasoline_consumption NUMERIC(3,1) CHECK ((is_electric AND gasoline_consumption IS NULL) OR (NOT is_electric AND gasoline_consumption > 0)),
	CONSTRAINT model_brand_uq UNIQUE(model_name, brand_id)
);

CREATE TABLE car_shop.color(
	color_id SERIAL NOT NULL PRIMARY KEY,
	color_name VARCHAR UNIQUE
);

CREATE TABLE car_shop.car(
	car_id SERIAL NOT NULL PRIMARY KEY,
	model_id INTEGER NOT NULL REFERENCES car_shop.model(model_id),
	color_id INTEGER NOT NULL REFERENCES car_shop.color(color_id),
	price NUMERIC(9, 2) NOT NULL
);

CREATE TABLE car_shop.customer(
	customer_id SERIAL NOT NULL PRIMARY KEY,
	first_name VARCHAR NOT NULL,
	last_name VARCHAR NOT NULL,
	salutation VARCHAR,
	title VARCHAR,
	phone VARCHAR NOT NULL,
	phone_extension VARCHAR	
);

CREATE TABLE car_shop.sale(
	sale_id SERIAL NOT NULL PRIMARY KEY,
	car_id INTEGER NOT NULL REFERENCES car_shop.car(car_id),
	customer_id INTEGER NOT NULL REFERENCES car_shop.customer(customer_id),
	sale_date DATE NOT NULL,
	discount NUMERIC(2, 0) NOT NULL CHECK (discount BETWEEN 0 AND 99),
	final_price NUMERIC(9, 2) NOT NULL
);

-- 1.3 Наполнение данными
INSERT INTO car_shop.country(country_name)
SELECT DISTINCT brand_origin
  FROM raw_data.sales
 WHERE brand_origin IS NOT NULL;

INSERT INTO car_shop.brand(brand_name, origin_country_id)
SELECT DISTINCT
       substr(s.auto, 1, strpos(s.auto, ' ') - 1) brand_name,
       c.country_id origin_country_id
  FROM raw_data.sales s
       LEFT JOIN car_shop.country c ON c.country_name = s.brand_origin;

INSERT INTO car_shop.model(
	model_name,
	brand_id,
	is_electric,
	gasoline_consumption)
SELECT DISTINCT
       substr(s.auto, strpos(s.auto, ' ') + 1, strpos(s.auto, ',') - strpos(s.auto, ' ') - 1) model_name,
       b.brand_id,
       s.gasoline_consumption IS NULL,
       s.gasoline_consumption
  FROM raw_data.sales s
       JOIN car_shop.brand b ON b.brand_name = substr(s.auto, 1, strpos(s.auto, ' ') - 1);
 
INSERT INTO car_shop.color(color_name)
SELECT DISTINCT
       initcap(substr(s.auto, strpos(s.auto, ',') + 2)) color_name
  FROM raw_data.sales s;

INSERT INTO car_shop.car(
	car_id,
	model_id,
	color_id,
	price)
SELECT s.id,
       m.model_id,
       c.color_id,
       round(s.price::numeric / (1 - (s.discount::numeric / 100)), 2)
  FROM raw_data.sales s
       JOIN car_shop.brand b ON b.brand_name = substr(s.auto, 1, strpos(s.auto, ' ') - 1)
       JOIN car_shop.model m ON m.brand_id = b.brand_id 
                            AND m.model_name = substr(s.auto, strpos(s.auto, ' ') + 1, strpos(s.auto, ',') - strpos(s.auto, ' ') - 1)
       JOIN car_shop.color c ON c.color_name = initcap(substr(s.auto, strpos(s.auto, ',') + 2));

-- В этой схеме каждой продаже соответствует машина, поэтому я использую один и тот же идентификатор в таблицах car и sale.
-- Из-за этого нужно подвинуть последовательность после заполнения таблицы.
ALTER SEQUENCE car_shop.car_car_id_seq RESTART WITH 1001;

INSERT INTO car_shop.customer(
	first_name,
	last_name,
	salutation,
	title,
	phone,
	phone_extension)
SELECT substr(z.clean_name, 1, strpos(z.clean_name, ' ') - 1) first_name,
       substr(z.clean_name, strpos(z.clean_name, ' ') + 1) first_name,
       CASE
           WHEN is_salutation THEN raw_salutation
       END salutation,
       CASE 
       	   WHEN z.is_title THEN z.raw_title
       END title,
       z.phone,
       z.phone_extension
  FROM (SELECT y.*,
	trim(CASE
		     WHEN is_title THEN replace(CASE
			                                WHEN is_salutation THEN replace(person, raw_salutation, '')
									        ELSE person
								        END, raw_title, '')
	 	     ELSE CASE
		 	          WHEN is_salutation THEN replace(person, raw_salutation, '')
	 	              ELSE person
		 	      END
	END) clean_name
  FROM (SELECT x.*,
               CASE WHEN raw_salutation IN ('Mr.', 'Mrs.', 'Dr.') THEN TRUE ELSE FALSE END is_salutation,
		       CASE WHEN raw_title IN ('MD', 'DVM', 'DDS') THEN TRUE ELSE FALSE END is_title
		  FROM (SELECT DISTINCT
		               s.person,
				       substr(s.person, 1, strpos(s.person, '.')) raw_salutation,
				       split_part(s.person, ' ', -1) raw_title,
				       split_part(s.phone, 'x', 1) phone,
				       CASE
					       WHEN length(split_part(s.phone, 'x', 2)) > 0 THEN split_part(s.phone, 'x', 2)
					       ELSE NULL
					   END phone_extension
		  		  FROM raw_data.sales s) x) y) z;

INSERT INTO car_shop.sale(
	sale_id,
	car_id,
	customer_id,
	sale_date,
	discount,
	final_price)
SELECT s.id,
       s.id,
       c.customer_id,
       s.sale_date,
       s.discount,
       round(s.price::numeric, 2)
  FROM raw_data.sales s
       JOIN car_shop.customer c ON trim(concat_ws(' ', c.salutation, c.first_name, c.last_name, c.title)) = s.person
                                AND CASE
	                                    WHEN c.phone_extension IS NOT NULL THEN c.phone||'x'||c.phone_extension
	                                    ELSE c.phone
	                                END = s.phone;

ALTER SEQUENCE car_shop.sale_sale_id_seq RESTART WITH 1001;


-- Этап 2. Создание выборок

---- Задание 1. Напишите запрос, который выведет процент моделей машин, у которых нет параметра `gasoline_consumption`.
SELECT (SUM(CASE WHEN gasoline_consumption IS NULL THEN 1 ELSE 0 END)::numeric / COUNT(1)::NUMERIC) * 100 nulls_percentage_gasoline_consumption
  FROM car_shop.model m;


---- Задание 2. Напишите запрос, который покажет название бренда и среднюю цену его автомобилей в разбивке по всем годам с учётом скидки.
-- Т.к. продаж какого-то бренда за год может не быть, то чтобы год не был пропущен, генерирую все нужные года
SELECT b.brand_name,
       y "year",
       sl.avg_price price_avg
  FROM car_shop.brand b
       CROSS JOIN generate_series(2015, 2023, 1) y
      LEFT JOIN (SELECT m.brand_id,
                        EXTRACT(YEAR FROM s.sale_date) year_sale,
                        round(avg(s.final_price), 2) avg_price
                   FROM car_shop.model m
                   JOIN car_shop.car c USING(model_id)
                   JOIN car_shop.sale s USING(car_id)
                  GROUP BY m.brand_id, EXTRACT(YEAR FROM s.sale_date)) sl ON sl.brand_id = b.brand_id
                                                                         AND sl.year_sale = y
 ORDER BY 1, 2;


---- Задание 3. Посчитайте среднюю цену всех автомобилей с разбивкой по месяцам в 2022 году с учётом скидки.
SELECT EXTRACT(MONTH FROM s.sale_date) "month",
       EXTRACT(YEAR FROM s.sale_date) "year",
       ROUND(AVG(s.final_price), 2) price_avg
  FROM car_shop.sale s
 WHERE s.sale_date BETWEEN '2022-01-01' AND '2022-12-31'
 GROUP BY EXTRACT(MONTH FROM s.sale_date), EXTRACT(YEAR FROM s.sale_date)
 ORDER BY 1;


---- Задание 4. Напишите запрос, который выведет список купленных машин у каждого пользователя.
SELECT c.first_name||' '||c.last_name person,
       STRING_AGG(b.brand_name||' '||m.model_name, ', ') cars
  FROM car_shop.sale s
       JOIN car_shop.customer c USING(customer_id)
       JOIN car_shop.car car USING(car_id)
       JOIN car_shop.model m USING(model_id)
       JOIN car_shop.brand b USING(brand_id)
 GROUP BY c.first_name||' '||c.last_name
 ORDER BY 1;

--- Задание 5. Напишите запрос, который вернёт самую большую и самую маленькую цену продажи автомобиля с разбивкой по стране
SELECT cnt.country_name brand_origin,
       MAX(car.price) price_max,
       MIN(car.price) price_min
  FROM car_shop.country cnt
       JOIN car_shop.brand b ON b.origin_country_id  = cnt.country_id
       JOIN car_shop.model m USING(brand_id)
       JOIN car_shop.car car USING(model_id)
       JOIN car_shop.sale s USING(car_id)
 GROUP BY cnt.country_name;

---- Задание 6. Напишите запрос, который покажет количество всех пользователей из США.
 SELECT COUNT(1) persons_from_usa_count
   FROM car_shop.customer c
  WHERE c.phone LIKE '+1%';


