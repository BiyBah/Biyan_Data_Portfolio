TRUNCATE TABLE car CASCADE;
TRUNCATE TABLE model CASCADE;
TRUNCATE TABLE bid CASCADE;
TRUNCATE TABLE ads CASCADE;
TRUNCATE TABLE users CASCADE;
TRUNCATE TABLE city CASCADE;
TRUNCATE TABLE province CASCADE;
TRUNCATE TABLE transmission CASCADE;
TRUNCATE TABLE mfg_year CASCADE;
TRUNCATE TABLE country CASCADE;
TRUNCATE TABLE body CASCADE;
TRUNCATE TABLE brand CASCADE;

COPY transmission
FROM '/home/biyanbahtiar/portfolio/Biyan-Bahtiar-Data-Analytics-Portfolio/project/DataEngineer/used_car_ecommerce_database/transmission.csv'
DELIMITER ','
CSV
HEADER;

COPY country
FROM '/home/biyanbahtiar/portfolio/Biyan-Bahtiar-Data-Analytics-Portfolio/project/DataEngineer/used_car_ecommerce_database/country.csv'
DELIMITER ','
CSV
HEADER;

COPY mfg_year
FROM '/home/biyanbahtiar/portfolio/Biyan-Bahtiar-Data-Analytics-Portfolio/project/DataEngineer/used_car_ecommerce_database/mfg_year.csv'
DELIMITER ','
CSV
HEADER;

COPY body
FROM '/home/biyanbahtiar/portfolio/Biyan-Bahtiar-Data-Analytics-Portfolio/project/DataEngineer/used_car_ecommerce_database/body.csv'
DELIMITER ','
CSV
HEADER;

COPY brand
FROM '/home/biyanbahtiar/portfolio/Biyan-Bahtiar-Data-Analytics-Portfolio/project/DataEngineer/used_car_ecommerce_database/brand.csv'
DELIMITER ','
CSV
HEADER;

COPY province
FROM '/home/biyanbahtiar/portfolio/Biyan-Bahtiar-Data-Analytics-Portfolio/project/DataEngineer/used_car_ecommerce_database/province.csv'
DELIMITER ','
CSV
HEADER;

COPY city
FROM '/home/biyanbahtiar/portfolio/Biyan-Bahtiar-Data-Analytics-Portfolio/project/DataEngineer/used_car_ecommerce_database/city.csv'
DELIMITER ','
CSV
HEADER;

COPY users
FROM '/home/biyanbahtiar/portfolio/Biyan-Bahtiar-Data-Analytics-Portfolio/project/DataEngineer/used_car_ecommerce_database/users.csv'
DELIMITER ','
CSV
HEADER;

COPY ads
FROM '/home/biyanbahtiar/portfolio/Biyan-Bahtiar-Data-Analytics-Portfolio/project/DataEngineer/used_car_ecommerce_database/ads.csv'
DELIMITER ','
CSV
HEADER;

COPY model
FROM '/home/biyanbahtiar/portfolio/Biyan-Bahtiar-Data-Analytics-Portfolio/project/DataEngineer/used_car_ecommerce_database/model.csv'
DELIMITER ','
CSV
HEADER;

COPY car
FROM '/home/biyanbahtiar/portfolio/Biyan-Bahtiar-Data-Analytics-Portfolio/project/DataEngineer/used_car_ecommerce_database/car.csv'
DELIMITER ','
CSV
HEADER;

COPY bid
FROM '/home/biyanbahtiar/portfolio/Biyan-Bahtiar-Data-Analytics-Portfolio/project/DataEngineer/used_car_ecommerce_database/bid.csv'
DELIMITER ','
CSV
HEADER;