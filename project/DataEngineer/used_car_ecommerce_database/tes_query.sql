-- Transactional Query --
-- 1. cari mobil keluaran tahun 2015 ke atas
SELECT c.car_id, b.brand, m.model_name as "car_name", mf.mfg_year as "year", c.price
FROM car c
LEFT JOIN model m
ON c.model_id = m.model_id
LEFT JOIN mfg_year mf
ON c.mfg_year_id = mf.mfg_year_id
LEFT JOIN brand b
ON m.brand_id = b.brand_id
WHERE mf.mfg_year >= 2015;

-- 2. menambahkan data bid baru
INSERT INTO bid
VALUES (14001, 1000000000, 80005, 12067, '2024-10-27');

SELECT *
FROM bid
ORDER BY bid_id DESC
LIMIT 5;

-- 3. melihat mobil yang dijual oleh salah satu akun, urut dari yang paling baru
SELECT
	u.user_id
	, COUNT(c.car_id) as count_car
FROM users u
LEFT JOIN ads a
ON u.user_id = a.user_id
LEFT JOIN car c
ON a.ads_id = c.ads_id
GROUP BY 1
ORDER BY 2 DESC;

SELECT u.user_name, b.brand, m.model_name as car_name, mf.mfg_year as "year", c.price, c.date_created
FROM car c
LEFT JOIN model m
ON c.model_id = m.model_id
LEFT JOIN mfg_year mf
ON c.mfg_year_id = mf.mfg_year_id
LEFT JOIN brand b
ON m.brand_id = b.brand_id
LEFT JOIN ads a
ON c.ads_id = a.ads_id
LEFT JOIN users u
ON a.user_id = u.user_id
WHERE u.user_id = 80142
ORDER BY c.date_created DESC
;

-- 4. Mencari mobil bekas berdasarkan keyword "Yaris"
SELECT 
	c.car_id
	, b.brand
	, m.model_name as car_name
	, c.color
	, mf.mfg_year as "year"
	, c.price
	, c.mileage_km
FROM car c
LEFT JOIN model m
ON c.model_id = m.model_id
LEFT JOIN brand b
ON m.brand_id = b.brand_id
LEFT JOIN mfg_year mf
ON c.mfg_year_id = mf.mfg_year_id
WHERE m.model_name ILIKE '%Yaris%';

-- 5. mencari mobil terdekat berdasarkan salah satu kota
SELECT 
	c.car_id
	, b.brand
	, m.model_name as car_name
	, mf.mfg_year as "year"
	, c.price
	, c.mileage_km
	, ci.city
	, haversine_distance((SELECT coordinate FROM city WHERE city = 'Bandung'), ci.coordinate)
FROM car c
LEFT JOIN model m
ON c.model_id = m.model_id
LEFT JOIN brand b
ON m.brand_id = b.brand_id
LEFT JOIN mfg_year mf
ON c.mfg_year_id = mf.mfg_year_id
LEFT JOIN ads a
ON c.ads_id = a.ads_id
LEFT JOIN users u
ON a.user_id = u.user_id
LEFT JOIN city ci
ON u.city_id = ci.city_id
ORDER BY haversine_distance ASC
LIMIT 5;

-- Analytical Query --
-- 6. Rank popularitas model mobil dengan jumlah bid terbanyak
SELECT
	b.brand
	, m.model_name as car_name
	, COUNT(DISTINCT c.car_id) as jumlah_mobil
	, COUNT(bi.bid_id) as jumlah_bid
FROM car c
LEFT JOIN model m
ON c.model_id = m.model_id
LEFT JOIN brand b
ON m.brand_id = b.brand_id
LEFT JOIN bid bi
ON c.car_id = bi.car_id
GROUP BY 1,2
ORDER BY 4 DESC;

-- 7. membandingkan harga mobil berdasarkan harga rata-rata per kota
SELECT
	b.brand
	, m.model_name as car_name
	, ci.city
	, ROUND(AVG(c.price),2) as avg_price
FROM car c
LEFT JOIN model m
ON c.model_id = m.model_id
LEFT JOIN brand b
ON m.brand_id = b.brand_id
LEFT JOIN ads a
ON c.ads_id = a.ads_id
LEFT JOIN users u
ON a.user_id = u.user_id
LEFT JOIN city ci
ON u.city_id = ci.city_id
GROUP BY 1,2,3
ORDER BY 2 ASC;

-- 8. membandingkan harga bid pertama dan harga bid terakhir untuk suatu model mobil per user
SELECT
	m.model_id
	, m.model_name
	, COUNT(bid_id)
FROM model m
LEFT JOIN car c
ON m.model_id = c.model_id
LEFT JOIN bid bi
ON c.car_id = bi.car_id
GROUP BY 1
ORDER BY 3 DESC;

WITH base_bid AS(
	SELECT
		m.model_name
		, bi.user_id
		, bi.date_created
		, bi.bid_price
		, ROW_NUMBER() OVER(PARTITION BY bi.user_id ORDER BY bi.date_created ASC)
	FROM bid bi
	LEFT JOIN car c
	ON bi.car_id = c.car_id
	LEFT JOIN model m
	ON c.model_id = m.model_id
	WHERE m.model_name = 'Lexus RX'
	ORDER BY 3,2 ASC
),
first_bid AS(
	SELECT
		model_name
		, user_id
		, date_created as first_bid_date
		, bid_price as first_bid_price
	FROM base_bid
	WHERE "row_number" = 1
),
second_bid AS(
	SELECT
		model_name
		, user_id
		, date_created as next_bid_date
		, bid_price as next_bid_price
	FROM base_bid
	WHERE "row_number" = 2
)
SELECT 
	f.model_name
	, f.user_id
	, f.first_bid_date
	, s.next_bid_date
	, f.first_bid_price
	, COALESCE(s.next_bid_price, 0) as next_bid_price
FROM first_bid f
LEFT JOIN second_bid s
ON f.user_id = s.user_id
ORDER BY 6 DESC;

-- 9. persentase harga mobil berdasarkan harga bidnya selama 6 bulan ke belakang
WITH model_avg_price AS(
	SELECT 
		m.model_name
		, ROUND(COALESCE(AVG(c.price),0),2) as avg_price
	FROM model m
	LEFT JOIN car c
	ON m.model_id = c.model_id
	GROUP BY 1
),
model_avg6_bid AS(
	SELECT
		m.model_name
		, ROUND(COALESCE(AVG(bi.bid_price),0),2) as avg_bid6_month
	FROM model m
	LEFT JOIN car c
	ON m.model_id = c.model_id
	LEFT JOIN bid bi
	ON c.car_id = bi.car_id
	WHERE bi.date_created >= (SELECT MAX(date_created) FROM bid) - INTERVAL '6 months'
	GROUP BY 1
)
SELECT
	a.model_name
	, a.avg_price
	, COALESCE(b.avg_bid6_month,0) as avg_bid6_month
	, (a.avg_price - COALESCE(b.avg_bid6_month,0)) as difference
	, ROUND((a.avg_price - COALESCE(b.avg_bid6_month,0))/NULLIF(a.avg_price,0)*100,2) as difference_percent
FROM model_avg_price a
LEFT JOIN model_avg6_bid b
ON a.model_name = b.model_name;

-- 10. membuat harga bid rata-rata untuk sebuah brand dan model tertentu selama 6 bulan terakhir (6 bln, 5 bln, 4 bln, 3 bln, 2 bln, 1 bln)
WITH model_avg_bid AS(
	SELECT
		m.model_name
		, (DATE(DATE_TRUNC('month', bi.date_created))) as "date"
		, AVG(bi.bid_price) OVER(ORDER BY DATE(DATE_TRUNC('month', bi.date_created)) DESC) as avg_price
		, DENSE_RANK() OVER(ORDER BY DATE(DATE_TRUNC('month', bi.date_created)) DESC) as "rank"
	FROM bid bi
	LEFT JOIN car c
	ON bi.car_id = c.car_id
	LEFT JOIN model m
	ON c.model_id = m.model_id
	WHERE 
		m.model_name ILIKE '%Santa Fe%' 
		AND 
		DATE(DATE_TRUNC('month', bi.date_created)) >= (SELECT MAX(DATE(DATE_TRUNC('month', bi.date_created))) FROM bid bi LEFT JOIN car c ON bi.car_id = c.car_id LEFT JOIN model m ON c.model_id = m.model_id WHERE m.model_name ILIKE '%Santa Fe%') - INTERVAL '5 months'
	ORDER BY 2 DESC
)
SELECT
	model_name
	, MIN(avg_price) FILTER (WHERE "rank" = 6) as m_min_6
	, MIN(avg_price) FILTER (WHERE "rank" = 5) as m_min_5
	, MIN(avg_price) FILTER (WHERE "rank" = 4) as m_min_4
	, MIN(avg_price) FILTER (WHERE "rank" = 3) as m_min_3
	, MIN(avg_price) FILTER (WHERE "rank" = 2) as m_min_2
	, MIN(avg_price) FILTER (WHERE "rank" = 1) as m_min_1
FROM model_avg_bid
GROUP BY 1;