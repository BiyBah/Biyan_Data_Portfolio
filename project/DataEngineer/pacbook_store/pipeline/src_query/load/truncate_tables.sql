CREATE SCHEMA IF NOT EXISTS staging;
--
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'staging' AND tablename = 'country') THEN
        EXECUTE 'TRUNCATE TABLE staging.country CASCADE';
    END IF;
END
$$;
--
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'staging' AND tablename = 'address_status') THEN
        EXECUTE 'TRUNCATE TABLE staging.address_status CASCADE';
    END IF;
END
$$;
--
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'staging' AND tablename = 'author') THEN
        EXECUTE 'TRUNCATE TABLE staging.author CASCADE';
    END IF;
END
$$;
--
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'staging' AND tablename = 'book_language') THEN
        EXECUTE 'TRUNCATE TABLE staging.book_language CASCADE';
    END IF;
END
$$;
--
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'staging' AND tablename = 'publisher') THEN
        EXECUTE 'TRUNCATE TABLE staging.publisher CASCADE';
    END IF;
END
$$;
--
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'staging' AND tablename = 'shipping_method') THEN
        EXECUTE 'TRUNCATE TABLE staging.shipping_method CASCADE';
    END IF;
END
$$;
--
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'staging' AND tablename = 'order_status') THEN
        EXECUTE 'TRUNCATE TABLE staging.order_status CASCADE';
    END IF;
END
$$;
--
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'staging' AND tablename = 'customer') THEN
        EXECUTE 'TRUNCATE TABLE staging.customer CASCADE';
    END IF;
END
$$;
--
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'staging' AND tablename = 'order_history') THEN
        EXECUTE 'TRUNCATE TABLE staging.order_history CASCADE';
    END IF;
END
$$;
--
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'staging' AND tablename = 'order_line') THEN
        EXECUTE 'TRUNCATE TABLE staging.order_line CASCADE';
    END IF;
END
$$;
--
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'staging' AND tablename = 'book_author') THEN
        EXECUTE 'TRUNCATE TABLE staging.book_author CASCADE';
    END IF;
END
$$;
--
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'staging' AND tablename = 'customer_address') THEN
        EXECUTE 'TRUNCATE TABLE staging.customer_address CASCADE';
    END IF;
END
$$;
--
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'staging' AND tablename = 'address') THEN
        EXECUTE 'TRUNCATE TABLE staging.address CASCADE';
    END IF;
END
$$;
--
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'staging' AND tablename = 'book') THEN
        EXECUTE 'TRUNCATE TABLE staging.book CASCADE';
    END IF;
END
$$;
--
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'staging' AND tablename = 'cust_order') THEN
        EXECUTE 'TRUNCATE TABLE staging.cust_order CASCADE';
    END IF;
END
$$;