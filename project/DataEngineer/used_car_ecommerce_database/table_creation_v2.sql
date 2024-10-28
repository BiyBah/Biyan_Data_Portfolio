-- drop table if exists --
DROP TABLE IF EXISTS model CASCADE;
DROP TABLE IF EXISTS car CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS ads CASCADE;
DROP TABLE IF EXISTS bid CASCADE;
DROP TABLE IF EXISTS city CASCADE;
DROP TABLE IF EXISTS province CASCADE;
DROP TABLE IF EXISTS country CASCADE;
DROP TABLE IF EXISTS mfg_year CASCADE;
DROP TABLE IF EXISTS transmission CASCADE;
DROP TABLE IF EXISTS body CASCADE;
DROP TABLE IF EXISTS brand CASCADE;

-- create table user --
CREATE TABLE users(
	user_id INTEGER PRIMARY KEY
	, user_name VARCHAR(100) UNIQUE NOT NULL
	, password VARCHAR(8) NOT NULL
	, phone_number VARCHAR(20) NOT NULL
	, social_media_address VARCHAR(100) NOT NULL
	, domicile_address VARCHAR(100) NOT NULL
	, city_id INTEGER NOT NULL
	, date_created DATE NOT NULL
)
;
-- create table province
CREATE TABLE province
(
	province_id INTEGER PRIMARY KEY
	, province VARCHAR(100) UNIQUE NOT NULL
	, country_id INTEGER NOT NULL
	, date_created DATE NOT NULL
)
;
-- create table city
CREATE TABLE city
(
	city_id INTEGER PRIMARY KEY
	, city VARCHAR(100) UNIQUE NOT NULL
	, province_id INTEGER NOT NULL
	, coordinate POINT NOT NULL
	, date_created DATE NOT NULL
)
;
-- create table country
CREATE TABLE country
(
	country_id INTEGER PRIMARY KEY
	, country VARCHAR(100) UNIQUE NOT NULL
	, date_created DATE NOT NULL
)
;
-- create table ads
CREATE TABLE ads
(
	ads_id INTEGER PRIMARY KEY
	, title VARCHAR(100) NOT NULL
	, description VARCHAR(100) NOT NULL
	, user_id INTEGER NOT NULL
	, date_created DATE NOT NULL
)
;
-- create table car
CREATE TABLE car
(
	car_id INTEGER PRIMARY KEY
	, permit_bid BOOLEAN NOT NULL
	, color VARCHAR(20)
	, mileage_km VARCHAR(20)
	, mfg_year_id INTEGER NOT NULL
	, model_id INTEGER NOT NULL
	, ads_id INTEGER NOT NULL
	, price INTEGER NOT NULL
	, date_created DATE NOT NULL
)
;
-- create table mfg_year
CREATE TABLE mfg_year
(
	mfg_year_id INTEGER PRIMARY KEY
	, mfg_year INTEGER NOT NULL
	, date_created DATE NOT NULL
)
;
-- create table model
CREATE TABLE model
(
	model_id INTEGER PRIMARY KEY
	, model_name VARCHAR(100) NOT NULL
	, transmission_id INTEGER NOT NULL
	, body_id INTEGER NOT NULL
	, brand_id INTEGER NOT NULL
	, date_created DATE NOT NULL
)
;
-- create table transmission
CREATE TABLE transmission
(
	transmission_id INTEGER PRIMARY KEY
	, transmission TEXT NOT NULL
	, date_created DATE NOT NULL
)
;
-- create table body
CREATE TABLE body
(
	body_id INTEGER PRIMARY KEY
	, body VARCHAR(50) NOT NULL
	, date_created DATE NOT NULL
)
;
-- create table brand
CREATE TABLE brand
(
	brand_id INTEGER PRIMARY KEY
	, brand VARCHAR(20) NOT NULL
	, date_created DATE NOT NULL
)
;
-- create table bid
CREATE TABLE bid
(
	bid_id INTEGER PRIMARY KEY
	, bid_price INTEGER NOT NULL CHECK(bid_price>0)
	, user_id INTEGER NOT NULL
	, car_id INTEGER NOT NULL
	, date_created DATE NOT NULL
)
;
-- alter table: add additional constraint
ALTER TABLE users
ADD CONSTRAINT fk_city
	FOREIGN KEY(city_id)
	REFERENCES city(city_id)
	ON DELETE CASCADE
	ON UPDATE CASCADE
;
ALTER TABLE city
ADD CONSTRAINT fk_province
	FOREIGN KEY(province_id)
	REFERENCES province(province_id)
	ON DELETE NO ACTION
	ON UPDATE CASCADE
;
ALTER TABLE province
ADD CONSTRAINT fk_country
	FOREIGN KEY(country_id)
	REFERENCES country(country_id)
	ON DELETE NO ACTION
	ON UPDATE CASCADE
;
ALTER TABLE ads
ADD CONSTRAINT fk_user
	FOREIGN KEY(user_id)
	REFERENCES users(user_id)
	ON DELETE CASCADE
	ON UPDATE CASCADE
;
ALTER TABLE car
ADD CONSTRAINT fk_ads
	FOREIGN KEY(ads_id)
	REFERENCES ads(ads_id)
	ON DELETE CASCADE
	ON UPDATE CASCADE
;
ALTER TABLE car
ADD CONSTRAINT fk_mfg_year
	FOREIGN KEY(mfg_year_id)
	REFERENCES mfg_year(mfg_year_id)
	ON DELETE NO ACTION
	ON UPDATE CASCADE
;
ALTER TABLE car
ADD CONSTRAINT fk_model
	FOREIGN KEY(model_id)
	REFERENCES model(model_id)
	ON DELETE NO ACTION
	ON UPDATE CASCADE
;
ALTER TABLE car
ALTER COLUMN permit_bid
SET DEFAULT FALSE
;
ALTER TABLE model
ADD CONSTRAINT fk_transmission
	FOREIGN KEY(transmission_id)
	REFERENCES transmission(transmission_id)
	ON DELETE NO ACTION
	ON UPDATE CASCADE
;
ALTER TABLE model
ADD CONSTRAINT fk_body
	FOREIGN KEY(body_id)
	REFERENCES body(body_id)
	ON DELETE NO ACTION
	ON UPDATE CASCADE
;
ALTER TABLE model
ADD CONSTRAINT fk_brand
	FOREIGN KEY(brand_id)
	REFERENCES brand(brand_id)
	ON DELETE NO ACTION
	ON UPDATE CASCADE
;
ALTER TABLE bid
ADD CONSTRAINT fk_user
	FOREIGN KEY(user_id)
	REFERENCES users(user_id)
	ON DELETE CASCADE
	ON UPDATE CASCADE
;
ALTER TABLE bid
ADD CONSTRAINT fk_car
	FOREIGN KEY(car_id)
	REFERENCES car(car_id)
	ON DELETE CASCADE
	ON UPDATE CASCADE
;
-- create function for password check
CREATE OR REPLACE FUNCTION password_check(passwd VARCHAR)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS
$$
BEGIN
	IF LENGTH(passwd) < 8 THEN
		RAISE EXCEPTION 'Password harus terdiri dari 8 karakter';
	END IF;

	IF passwd !~ '[0-9]' THEN
		RAISE EXCEPTION 'Password harus berisi minimal 1 angka';
	END IF;

	IF passwd !~ '[A-Z]' THEN
		RAISE EXCEPTION 'Password harus berisi minimal 1 huruf kapital';
	END IF;

	IF passwd !~ '[a-z]' THEN
		RAISE EXCEPTION 'Password harus berisi minimal 1 huruf kecil';
	END IF;

	RETURN TRUE;
END;
$$
;
CREATE OR REPLACE FUNCTION enforce_password_policy()
RETURNS TRIGGER 
LANGUAGE plpgsql
AS 
$$
BEGIN
    PERFORM password_check(NEW.password);
    RETURN NEW;
END;
$$
;
CREATE TRIGGER check_password
BEFORE INSERT OR UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION enforce_password_policy()
;

-- create haversine distance function
CREATE OR REPLACE FUNCTION haversine_distance(point1 POINT, point2 POINT)
RETURNS float
LANGUAGE plpgsql
AS
$$
DECLARE
	lon1 float := radians(point1[0]);
	lat1 float := radians(point1[1]);
	lon2 float := radians(point2[0]);
	lat2 float := radians(point2[1]);

	dlon float := lon2 - lon1;
	dlat float := lat2 - lat1;

	a float;
	c float;
	r float := 6371;
	jarak float;
BEGIN
	a := sin(dlat/2)^2 * cos(lat1) * cos(lat2) * sin(dlon/2)^2;
	c := 2 * asin(sqrt(a));
	jarak := r*c;
	RETURN jarak;
END;
$$
;