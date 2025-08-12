\c warehouse;

DROP TABLE IF EXISTS dim_people;
DROP TABLE IF EXISTS dim_company;
DROP TABLE IF EXISTS dim_relationships;
DROP TABLE IF EXISTS dim_acquisition;
DROP TABLE IF EXISTS dim_ipos;
DROP TABLE IF EXISTS dim_funds;
DROP TABLE IF EXISTS dim_funding_rounds;
DROP TABLE IF EXISTS dim_investments;
DROP TABLE IF EXISTS dim_milestones;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_event_type;
DROP TABLE IF EXISTS fct_startup_event;

CREATE TABLE public.dim_people (
    people_id VARCHAR(255) UNIQUE NOT NULL,
    people_nk VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    birthplace VARCHAR(255),
    affiliation_name VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT dim_people_pkey PRIMARY KEY (people_id)
);

CREATE TABLE public.dim_company (
    company_id VARCHAR(255) UNIQUE NOT NULL,
    office_nk INTEGER,
    object_nk VARCHAR(255),
    description TEXT,
    region VARCHAR(255),
    address1 TEXT,
    address2 TEXT,
    city VARCHAR(255),
    zip_code VARCHAR(200),
    state_code VARCHAR(255),
    country_code VARCHAR(255),
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT dim_company_pkey PRIMARY KEY (company_id)
);

CREATE TABLE public.dim_relationships (
    relationships_id VARCHAR(255) UNIQUE NOT NULL,
    people_id VARCHAR(255), 
    company_id VARCHAR(255), 
    relationship_nk VARCHAR(255), 
    person_object_nk VARCHAR(255), 
    relationship_object_nk VARCHAR(255), 
    start_at TIMESTAMP,
    end_at TIMESTAMP,
    is_past BOOLEAN,
    sequence INTEGER,
    title VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT dim_relationships_pkey PRIMARY KEY (relationships_id),
    CONSTRAINT fk_people_id_people
        FOREIGN KEY (people_id)
        REFERENCES dim_people (people_id)
        ON DELETE NO ACTION,
    CONSTRAINT fk_company_id_company
        FOREIGN KEY (company_id)
        REFERENCES dim_company (company_id)
        ON DELETE NO ACTION
);

CREATE TABLE public.dim_acquisition (
    acquisition_id VARCHAR(255) UNIQUE NOT NULL,
    acquisition_nk INTEGER NOT NULL, 
    acquiring_object_nk VARCHAR(255), 
    acquired_object_nk VARCHAR(255),
    term_code VARCHAR(255),
    price_amount NUMERIC(15,2),
    price_currency_code VARCHAR(3),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT dim_acquisition_pkey PRIMARY KEY (acquisition_id)
);


CREATE TABLE public.dim_ipos (
    ipos_id VARCHAR(255) UNIQUE NOT NULL,
    ipo_nk VARCHAR(255) NOT NULL,
    valuation_amount NUMERIC(15,2),
    valuation_currency_code VARCHAR(3),
    raised_amount NUMERIC(15,2),
    raised_currency_code VARCHAR(3),
    stock_symbol VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT dim_ipos_pkey PRIMARY KEY (ipos_id)
);

CREATE TABLE public.dim_funds (
    funds_id VARCHAR(255) UNIQUE NOT NULL,
    fund_nk VARCHAR(255) NOT NULL, 
    "name" VARCHAR(255), 
    raised_amount NUMERIC(15,2),
    raised_currency_code VARCHAR(3),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT dim_funds_pkey PRIMARY KEY (funds_id)
);

CREATE TABLE public.dim_funding_rounds (
    funding_rounds_id VARCHAR(255) UNIQUE NOT NULL,
    funding_round_nk INTEGER NOT NULL, 
    funding_round_type VARCHAR(255),
    funding_round_code VARCHAR(255),
    raised_amount_usd NUMERIC(15,2),
    raised_amount NUMERIC(15,2),
    raised_currency_code VARCHAR(255),
    pre_money_valuation_usd NUMERIC(15,2),
    pre_money_valuation NUMERIC(15,2),
    pre_money_currency_code VARCHAR(255),
    post_money_valuation_usd NUMERIC(15,2),
    post_money_valuation NUMERIC(15,2),
    post_money_currency_code VARCHAR(255),
    participants TEXT,
    is_first_round BOOLEAN,
    is_last_round BOOLEAN,
    created_by VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT dim_funding_rounds_pkey PRIMARY KEY (funding_rounds_id)
);

CREATE TABLE public.dim_investments (
    investments_id VARCHAR(255) UNIQUE NOT NULL,
    funding_rounds_id VARCHAR(255),
    investment_nk INTEGER NOT NULL, 
    funding_round_nk INTEGER, 
    funded_object_nk VARCHAR(255), 
    investor_object_nk VARCHAR(255), 
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT dim_investments_pkey PRIMARY KEY (investments_id),
    CONSTRAINT fk_funding_rounds_id_funding_rounds
        FOREIGN KEY (funding_rounds_id)
        REFERENCES dim_funding_rounds (funding_rounds_id)
        ON DELETE NO ACTION
);

CREATE TABLE public.dim_milestones (
    milestones_id VARCHAR(255) UNIQUE NOT NULL,
    milestone_nk INTEGER NOT NULL, 
    description VARCHAR(255),
    milestone_code VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT dim_milestones_pkey PRIMARY KEY (milestones_id)
);

CREATE TABLE public.dim_date (
    date_id INT UNIQUE NOT NULL, 
    date_actual DATE UNIQUE NOT NULL,
    day_suffix VARCHAR(4),
    day_name VARCHAR(9),
    day_of_year INT,
    week_of_month INT,
    week_of_year INT,
    week_of_year_iso CHAR(10),
    month_actual INT,
    month_name VARCHAR(9),
    month_name_abbreviated CHAR(3),
    quarter_actual INT,
    quarter_name VARCHAR(9),
    year_actual INT,
    first_day_of_week DATE,
    last_day_of_week DATE,
    first_day_of_month DATE,
    last_day_of_month DATE,
    first_day_of_quarter DATE,
    last_day_of_quarter DATE,
    first_day_of_year DATE,
    last_day_of_year DATE,
    mmyyyy CHAR(6),
    mmddyyyy CHAR(10),
    weekend_indr VARCHAR(20),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT dim_date_pkey PRIMARY KEY (date_id)
);

CREATE TABLE public.dim_event_type (
    event_type_id VARCHAR(255) UNIQUE NOT NULL, 
    event_type VARCHAR(20),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT dim_event_type_pkey PRIMARY KEY (event_type_id)
);

CREATE TABLE public.fct_startup_event (
    startup_event_id VARCHAR(255) UNIQUE NOT NULL,
    company_id VARCHAR(255), 
    acquisition_id VARCHAR(255), 
    ipos_id VARCHAR(255),
    funds_id VARCHAR(255),
    funding_rounds_id VARCHAR(255), 
    milestones_id VARCHAR(255),
    event_type_id VARCHAR(255),
    stable_id VARCHAR(255), 
    event_datetime TIMESTAMP,
    event_date DATE,
    source_url TEXT,
    source_description TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT fct_startup_event_pkey PRIMARY KEY (startup_event_id),
    CONSTRAINT fk_company_id_company
        FOREIGN KEY (company_id)
        REFERENCES dim_company (company_id)
        ON DELETE NO ACTION,
    CONSTRAINT fk_acquisition_id_acquisition
        FOREIGN KEY (acquisition_id)
        REFERENCES dim_acquisition (acquisition_id)
        ON DELETE NO ACTION,
    CONSTRAINT fk_ipos_id_ipos
        FOREIGN KEY (ipos_id)
        REFERENCES dim_ipos (ipos_id)
        ON DELETE NO ACTION,
    CONSTRAINT fk_funds_id_funds
        FOREIGN KEY (funds_id)
        REFERENCES dim_funds (funds_id)
        ON DELETE NO ACTION,
    CONSTRAINT fk_funding_rounds_id_funding_rounds
        FOREIGN KEY (funding_rounds_id)
        REFERENCES dim_funding_rounds (funding_rounds_id)
        ON DELETE NO ACTION,
    CONSTRAINT fk_milestones_id_milestones
        FOREIGN KEY (milestones_id)
        REFERENCES dim_milestones (milestones_id)
        ON DELETE NO ACTION,
    CONSTRAINT fk_event_type_id_event_type
        FOREIGN KEY (event_type_id)
        REFERENCES dim_event_type (event_type_id)
        ON DELETE NO ACTION,
    CONSTRAINT fk_event_date_date
        FOREIGN KEY (event_date)
        REFERENCES dim_date (date_actual)
        ON DELETE NO ACTION
);