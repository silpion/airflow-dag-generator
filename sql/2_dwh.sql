\c dwh;

CREATE SCHEMA load;
CREATE SCHEMA stage_fdw;

CREATE EXTENSION postgres_fdw;

CREATE SERVER stage
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host '127.0.0.1', port '5432', dbname 'stage');
    
CREATE USER MAPPING FOR root
    SERVER stage
    OPTIONS (user 'root', password 'poc_af_123##');
    
CREATE TABLE dim_customer (
    id serial PRIMARY KEY,
    nk bigint NOT NULL,
    first_name VARCHAR (255) UNIQUE NOT NULL,
    last_name VARCHAR (255) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR (255) NOT NULL DEFAULT 'admin',
    changed_at TIMESTAMP,
    changed_by VARCHAR (255)
);
CREATE UNIQUE INDEX idx_dim_customer_nk on public.dim_customer(nk);

CREATE TABLE dim_article (
    id serial PRIMARY KEY,
    nk BIGINT NOT NULL,
    name VARCHAR (255) NOT NULL,
    description VARCHAR (255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR (255) NOT NULL DEFAULT 'admin',
    changed_at TIMESTAMP,
    changed_by VARCHAR (255)
);
CREATE UNIQUE INDEX idx_dim_article_nk on public.dim_article(nk);

CREATE TABLE dim_store (
    id serial PRIMARY KEY,
    nk BIGINT NOT NULL,
    name VARCHAR (255) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR (255) NOT NULL DEFAULT 'admin',
    changed_at TIMESTAMP,
    changed_by VARCHAR (255)
);
CREATE UNIQUE INDEX idx_dim_store_nk on public.dim_store(nk);


CREATE TABLE fact_sale (
    id serial PRIMARY KEY,
    nk VARCHAR (255) NOT NULL,
    id_dim_customer BIGINT NOT NULL,
    id_dim_article BIGINT NOT NULL,
    id_dim_store BIGINT NOT NULL,
    id_dim_date INTEGER NOT NULL,
    id_dim_time INTEGER NOT NULL,
    amount BIGINT NOT NULL,
    value BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR (255) NOT NULL DEFAULT 'admin',
    changed_at TIMESTAMP,
    changed_by VARCHAR (255)
);
CREATE UNIQUE INDEX idx_fact_nk on public.fact_sale(nk);

---------------------------------

CREATE OR REPLACE
    PROCEDURE load.sp_dim_article()
    language plpgsql as $$
BEGIN
   INSERT INTO
      public.dim_article (nk, name, description, created_at, created_by)
      SELECT
         nk_number,
         name,
         description,
         current_timestamp,
         'etl'
      FROM
         stage_fdw.article
         ON CONFLICT (nk) DO
         UPDATE
         SET
            name = EXCLUDED.name,
            description = EXCLUDED.description,
            created_at = EXCLUDED.created_at,
            created_by = EXCLUDED.created_by ;
COMMIT;
END
;
 $$

;

CREATE OR REPLACE
    PROCEDURE load.sp_dim_customer()
    language plpgsql as $$
BEGIN
   INSERT INTO
      public.dim_customer (nk, first_name, last_name, created_at, created_by)
      SELECT
         nk_number,
         first_name,
         last_name,
         current_timestamp,
         'etl'
      FROM
         stage_fdw.customer
         ON CONFLICT (nk) DO
         UPDATE
         SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            created_at = EXCLUDED.created_at,
            created_by = EXCLUDED.created_by ;
COMMIT;
END
;
 $$
;

CREATE OR REPLACE
    PROCEDURE load.sp_dim_store()
    language plpgsql as $$
BEGIN
   INSERT INTO
      public.dim_store (nk, name, created_at, created_by)
      SELECT
         nk_number,
         name,
         current_timestamp,
         'etl'
      FROM
         stage_fdw.store
         ON CONFLICT (nk) DO
         UPDATE
         SET
            name = EXCLUDED.name,
            created_at = EXCLUDED.created_at,
            created_by = EXCLUDED.created_by ;
COMMIT;
END
;
 $$
;

CREATE OR REPLACE
    PROCEDURE load.sp_fact_sale()
    language plpgsql as $$
BEGIN
   INSERT INTO
      public.fact_sale (nk, id_dim_customer, id_dim_article, id_dim_store, id_dim_date, id_dim_time, amount, value, created_at, created_by)
      SELECT
        dcu.id || '.' || dar.id || '.' || dst.id || '.' || to_char(sale_date, 'YYYYMMDD') || '.' || to_char(sale_date, 'hh24mi') as nk,
        dcu.id as id_dim_customer, 
        dar.id as id_dim_article,
        dst.id as id_dim_store,
        to_char(sale_date, 'YYYYMMDD')::integer as id_dim_date,
        to_char(sale_date, 'hh24mi')::integer as id_dim_time,
        sa.amount,
        sa.price as value,
        current_timestamp created_at,
        'etl' as created_by
      FROM stage_fdw.sale as sa
      INNER JOIN public.dim_customer dcu
        ON sa.id_customer = dcu.id
      INNER JOIN public.dim_article dar
        ON sa.id_article = dar.id
      INNER JOIN public.dim_store dst
        ON sa.id_store = dst.id

      ON CONFLICT (nk) DO
         UPDATE
         SET
            amount = EXCLUDED.amount,
            value = EXCLUDED.value,
            created_at = EXCLUDED.created_at,
            created_by = EXCLUDED.created_by ;
COMMIT;
END
;
 $$
;

CREATE OR REPLACE VIEW public.dim_date AS
    SELECT
        to_char(datum, 'YYYYMMDD') as id,
        extract(year from datum) AS Year,
        extract(month from datum) AS Month,
        -- Localized month name
        to_char(datum, 'TMMonth') AS MonthName,
        extract(day from datum) AS Day,
        extract(doy from datum) AS DayOfYear,
        -- Localized weekday
        to_char(datum, 'TMDay') AS WeekdayName,
        -- ISO calendar week
        extract(week from datum) AS CalendarWeek,
        to_char(datum, 'dd. mm. yyyy') AS FormattedDate,
        'Q' || to_char(datum, 'Q') AS Quartal,
        to_char(datum, 'yyyy/"Q"Q') AS YearQuartal,
        to_char(datum, 'yyyy/mm') AS YearMonth,
        -- ISO calendar year and week
        to_char(datum, 'iyyy/IW') AS YearCalendarWeek,
        -- Weekend
        CASE WHEN extract(isodow from datum) in (6, 7) THEN 'Weekend' ELSE 'Weekday' END AS Weekend,
        -- Fixed holidays
            -- for America
            CASE WHEN to_char(datum, 'MMDD') IN ('0101', '0704', '1225', '1226')
            THEN 'Holiday' ELSE 'No holiday' END
            AS AmericanHoliday,
            -- for Austria
        CASE WHEN to_char(datum, 'MMDD') IN
            ('0101', '0106', '0501', '0815', '1101', '1208', '1225', '1226')
            THEN 'Holiday' ELSE 'No holiday' END
            AS AustrianHoliday,
            -- for Canada
            CASE WHEN to_char(datum, 'MMDD') IN ('0101', '0701', '1225', '1226')
            THEN 'Holiday' ELSE 'No holiday' END
            AS CanadianHoliday,
        -- Some periods of the year, adjust for your organisation and country
        CASE WHEN to_char(datum, 'MMDD') BETWEEN '0701' AND '0831' THEN 'Summer break'
            WHEN to_char(datum, 'MMDD') BETWEEN '1115' AND '1225' THEN 'Christmas season'
            WHEN to_char(datum, 'MMDD') > '1225' OR to_char(datum, 'MMDD') <= '0106' THEN 'Winter break'
            ELSE 'Normal' END
            AS Period,
        -- ISO start and end of the week of this date
        datum + (1 - extract(isodow from datum))::integer AS CWStart,
        datum + (7 - extract(isodow from datum))::integer AS CWEnd,
        -- Start and end of the month of this date
        datum + (1 - extract(day from datum))::integer AS MonthStart,
        (datum + (1 - extract(day from datum))::integer + '1 month'::interval)::date - '1 day'::interval AS MonthEnd
    FROM (
        -- There are 3 leap years in this range, so calculate 365 * 10 + 3 records
        SELECT '2000-01-01'::DATE + sequence.day AS datum
        FROM generate_series(0,3652) AS sequence(day)
        GROUP BY sequence.day
        ) DQ
    order by 1;

-- Taken from https://wiki.postgresql.org/wiki/Date_and_Time_dimensions

CREATE OR REPLACE VIEW public.dim_time AS
    select to_char(minute, 'hh24mi') AS id,
        -- Hour of the day (0 - 23)
        extract(hour from minute) as Hour,
        -- Extract and format quarter hours
        to_char(minute - (extract(minute from minute)::integer % 15 || 'minutes')::interval, 'hh24:mi') ||
        ' – ' ||
        to_char(minute - (extract(minute from minute)::integer % 15 || 'minutes')::interval + '14 minutes'::interval, 'hh24:mi')
            as QuarterHour,
        -- Minute of the day (0 - 1439)
        extract(hour from minute)*60 + extract(minute from minute) as minute,
        -- Names of day periods
        case when to_char(minute, 'hh24:mi') between '06:00' and '08:29'
            then 'Morning'
            when to_char(minute, 'hh24:mi') between '08:30' and '11:59'
            then 'AM'
            when to_char(minute, 'hh24:mi') between '12:00' and '17:59'
            then 'PM'
            when to_char(minute, 'hh24:mi') between '18:00' and '22:29'
            then 'Evening'
            else 'Night' 
        end as DaytimeName,
        -- Indicator of day or night
        case when to_char(minute, 'hh24:mi') between '07:00' and '19:59' then 'Day'
            else 'Night'
        end AS DayNight
    from (SELECT '0:00'::time + (sequence.minute || ' minutes')::interval AS minute
        FROM generate_series(0,1439) AS sequence(minute)
        GROUP BY sequence.minute
        ) DQ
    order by 1;

-- taken from https://wiki.postgresql.org/wiki/Date_and_Time_dimensions


