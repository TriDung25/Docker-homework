DROP SCHEMA IF EXISTS star CASCADE;
CREATE SCHEMA star;

-- DIM DATE: mỗi ngày một dòng
CREATE TABLE star.dim_date AS
SELECT to_char(d, 'YYYYMMDD')::int          AS date_key,
       d::date                              AS full_date,
       EXTRACT(year FROM d)::int            AS year,
       EXTRACT(quarter FROM d)::int         AS quarter,
       EXTRACT(month FROM d)::int           AS month,
       to_char(d, 'FMMonth')                AS month_name,
       EXTRACT(day FROM d)::int             AS day,
       EXTRACT(isodow FROM d)::int          AS day_of_week,
       to_char(d, 'FMDay')                  AS day_name,
       (EXTRACT(isodow FROM d) IN (6, 7))   AS is_weekend
FROM generate_series(
       (SELECT min(rental_date)::date FROM public.rental)::timestamp,
       (SELECT GREATEST(max(rental_date), max(return_date))::date FROM public.rental)::timestamp,
       interval '1 day') AS d;

-- DIM CUSTOMER: gộp customer + address + city + country
CREATE TABLE star.dim_customer AS
SELECT row_number() OVER (ORDER BY c.customer_id)  AS customer_key,
       c.customer_id,
       c.first_name || ' ' || c.last_name          AS customer_name,
       c.email,
       c.activebool                                AS is_active,
       a.district,
       ci.city,
       co.country
FROM public.customer c
LEFT JOIN public.address a  ON a.address_id = c.address_id
LEFT JOIN public.city ci    ON ci.city_id = a.city_id
LEFT JOIN public.country co ON co.country_id = ci.country_id;

-- DIM FILM: gộp film + language + category
CREATE TABLE star.dim_film AS
SELECT row_number() OVER (ORDER BY f.film_id)  AS film_key,
       f.film_id,
       f.title,
       f.release_year,
       trim(l.name)                            AS language,
       cat.name                                AS category,
       f.rating,
       f.rental_duration,
       f.rental_rate,
       f.length,
       f.replacement_cost
FROM public.film f
LEFT JOIN public.language l ON l.language_id = f.language_id
LEFT JOIN (SELECT DISTINCT ON (film_id) film_id, category_id
           FROM public.film_category
           ORDER BY film_id, category_id) fc ON fc.film_id = f.film_id
LEFT JOIN public.category cat ON cat.category_id = fc.category_id;

-- DIM STORE: gộp store + address + city + country + quản lý
CREATE TABLE star.dim_store AS
SELECT row_number() OVER (ORDER BY s.store_id)  AS store_key,
       s.store_id,
       a.address,
       a.district,
       ci.city,
       co.country,
       m.first_name || ' ' || m.last_name       AS manager_name
FROM public.store s
LEFT JOIN public.address a  ON a.address_id = s.address_id
LEFT JOIN public.city ci    ON ci.city_id = a.city_id
LEFT JOIN public.country co ON co.country_id = ci.country_id
LEFT JOIN public.staff m    ON m.staff_id = s.manager_staff_id;

-- DIM STAFF
CREATE TABLE star.dim_staff AS
SELECT row_number() OVER (ORDER BY st.staff_id)  AS staff_key,
       st.staff_id,
       st.first_name || ' ' || st.last_name      AS staff_name,
       st.email
FROM public.staff st;

-- FACT RENTAL: mỗi lượt thuê một dòng
CREATE TABLE star.fact_rental AS
SELECT r.rental_id,
       to_char(r.rental_date, 'YYYYMMDD')::int  AS rental_date_key,
       to_char(r.return_date, 'YYYYMMDD')::int  AS return_date_key,
       dc.customer_key,
       df.film_key,
       ds.store_key,
       dst.staff_key,
       COALESCE(p.amount, 0)                    AS amount,
       (r.return_date IS NOT NULL)              AS is_returned,
       CASE WHEN r.return_date IS NOT NULL
            THEN EXTRACT(epoch FROM (r.return_date - r.rental_date)) / 86400.0
       END                                      AS rental_days
FROM public.rental r
LEFT JOIN public.inventory i     ON i.inventory_id = r.inventory_id
LEFT JOIN star.dim_customer dc   ON dc.customer_id = r.customer_id
LEFT JOIN star.dim_film df       ON df.film_id = i.film_id
LEFT JOIN star.dim_store ds      ON ds.store_id = i.store_id
LEFT JOIN star.dim_staff dst     ON dst.staff_id = r.staff_id
LEFT JOIN (SELECT rental_id, SUM(amount) AS amount
           FROM public.payment
           GROUP BY rental_id) p ON p.rental_id = r.rental_id;

-- KHOÁ CHÍNH và KHOÁ NGOẠI
ALTER TABLE star.dim_date     ADD PRIMARY KEY (date_key);
ALTER TABLE star.dim_customer ADD PRIMARY KEY (customer_key);
ALTER TABLE star.dim_film     ADD PRIMARY KEY (film_key);
ALTER TABLE star.dim_store    ADD PRIMARY KEY (store_key);
ALTER TABLE star.dim_staff    ADD PRIMARY KEY (staff_key);
ALTER TABLE star.fact_rental  ADD PRIMARY KEY (rental_id);
ALTER TABLE star.fact_rental
  ADD FOREIGN KEY (rental_date_key) REFERENCES star.dim_date (date_key),
  ADD FOREIGN KEY (return_date_key) REFERENCES star.dim_date (date_key),
  ADD FOREIGN KEY (customer_key)    REFERENCES star.dim_customer (customer_key),
  ADD FOREIGN KEY (film_key)        REFERENCES star.dim_film (film_key),
  ADD FOREIGN KEY (store_key)       REFERENCES star.dim_store (store_key),
  ADD FOREIGN KEY (staff_key)       REFERENCES star.dim_staff (staff_key);