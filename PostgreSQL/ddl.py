"""
Data Definition Language. Contains tuples of SQL commands that define and create the schemas and tables
for the PostgreSQL database. Also contains SQL commands for normalizing data, resetting the database,
and joining port data to the immigration table.
"""

set_up_postgres = ("""CREATE SCHEMA IF NOT EXISTS raw""",
"""SET search_path TO raw""",
"""
CREATE TABLE IF NOT EXISTS raw.immigration(
cicid       FLOAT,
i94yr       FLOAT,
i94mon      FLOAT,
i94cit      FLOAT,
i94res      FLOAT,
i94port     VARCHAR,
arrdate     FLOAT,
i94mode     FLOAT,
i94addr     VARCHAR,
depdate     FLOAT,
i94bir      FLOAT,
i94visa     FLOAT,
count       FLOAT,
dtadfile    VARCHAR,
visapost    VARCHAR,
occup       VARCHAR,
entdepa     VARCHAR,
entdepd     VARCHAR,
entdepu     VARCHAR,
matflag     VARCHAR,
biryear     FLOAT,
dtaddto     VARCHAR,
gender      VARCHAR,
insnum      VARCHAR,
airline     VARCHAR,
admnum      FLOAT,
fltno       VARCHAR,
visatype    VARCHAR
)
""",

"""CREATE TABLE IF NOT EXISTS raw.demographics(
city                      VARCHAR,
state                     VARCHAR,
median_age                FLOAT,
male_population           INTEGER,
female_population         INTEGER,
total_population          INTEGER,
number_of_veterans        INTEGER,
foreign_born              VARCHAR,
average_household_size    VARCHAR,
state_code                VARCHAR,
race                      VARCHAR,
count                     VARCHAR
)
""",

"""CREATE TABLE IF NOT EXISTS raw.airport_codes(
ident            VARCHAR,
type             VARCHAR,
name             VARCHAR,
elevation_ft     VARCHAR,
continent        VARCHAR,
iso_country      VARCHAR,
iso_region       VARCHAR,
municipality     VARCHAR,
gps_code         VARCHAR,
iata_code        VARCHAR,
local_code       VARCHAR,
coordinates      VARCHAR
)
""",

"""CREATE TABLE IF NOT EXISTS raw.i94_country_codes(
i94code         INT,
country         VARCHAR
)
""",

"""CREATE TABLE IF NOT EXISTS raw.i94_port_codes(
i94_port_code       VARCHAR,
city                VARCHAR,
state               VARCHAR
)
""",

"""CREATE TABLE IF NOT EXISTS raw.global_temperatures(
dt                              VARCHAR,
AverageTemperature              VARCHAR,
AverageTemperatureUncertainty   VARCHAR,
Country                         VARCHAR
)
"""
)

replace_blanks_in_port_codes = (
"""UPDATE raw.i94_port_codes
SET "state" = NULL
WHERE "state" = ''"""
)

set_up_intermediate_schema = ("""CREATE SCHEMA IF NOT EXISTS int""",
"""SET search_path TO int""",
"""
CREATE TABLE IF NOT EXISTS int.immigration(
cicid               INT PRIMARY KEY,
year                INT,
month               INT,
citizenship_country VARCHAR,
residency_country   VARCHAR,
i94port_code        VARCHAR,
port_city           VARCHAR,
port_state          VARCHAR,
arrival_date        DATE,
arrival_mode        VARCHAR,
state               VARCHAR,
departure_date      DATE,
age_in_years        INT,
visa_purpose        VARCHAR,
date_added_to_files DATE,
visapost            VARCHAR,
occupation          VARCHAR,
arrival_flag        VARCHAR,
departure_flag      VARCHAR,
update_flag         VARCHAR,
match_flag          VARCHAR,
birth_year          INT,
admitted_until      DATE,
gender              VARCHAR,
insnum              VARCHAR,
airline             VARCHAR,
admnum              BIGINT,
flight_number       VARCHAR,
visa_type           VARCHAR
)
""",

"""CREATE TABLE IF NOT EXISTS int.demographics(
city                      VARCHAR,
state_name                VARCHAR,
median_age                NUMERIC(3,1),
male_population           INT,
female_population         INT,
total_population          INT,
number_of_veterans        INT,
foreign_born              INT,
average_household_size    NUMERIC(3,2),
state_code                VARCHAR
)
""",

"""CREATE TABLE IF NOT EXISTS int.cities(
city_id    SERIAL PRIMARY KEY,
city       VARCHAR,
state_code VARCHAR,
state_name VARCHAR
)
""",

"""CREATE TABLE IF NOT EXISTS int.racial_demographics(
city       VARCHAR,
state_code VARCHAR,
state_name VARCHAR,
race       VARCHAR,
"count"    INT
)
""",

"""CREATE TABLE IF NOT EXISTS int.airport_codes(
ident            VARCHAR PRIMARY KEY,
type             VARCHAR,
name             VARCHAR,
elevation_ft     INT,
continent        VARCHAR,
iso_country      VARCHAR,
iso_region       VARCHAR,
municipality     VARCHAR,
gps_code         VARCHAR,
iata_code        VARCHAR,
local_code       VARCHAR,
latitude         FLOAT,
longitude        FLOAT
)
""",

"""CREATE TABLE IF NOT EXISTS int.global_temperatures(
record_date                     DATE,
average_temperature             FLOAT,
average_temperature_uncertainty FLOAT,
country                         VARCHAR
)
"""
)

set_up_presentation_schema = ("""CREATE SCHEMA IF NOT EXISTS prs""",
"""SET search_path TO prs""",
"""
CREATE MATERIALIZED VIEW IF NOT EXISTS prs.immigration AS SELECT * FROM int.immigration
""",

"""CREATE MATERIALIZED VIEW IF NOT EXISTS prs.demographics AS 
SELECT d.median_age, d.male_population, d.female_population, d.total_population,
	   d.number_of_veterans, d.foreign_born, d.average_household_size, c.city, c.state_code, c.state_name,
	   r.race, r.count
FROM int.demographics d
JOIN int.cities c ON d.city_id = c.city_id
JOIN int.racial_demographics r ON r.city = c.city
""",

"""CREATE MATERIALIZED VIEW IF NOT EXISTS prs.cities AS SELECT * FROM int.cities
""",

"""CREATE MATERIALIZED VIEW IF NOT EXISTS prs.airport_codes AS SELECT * FROM int.airport_codes
""",

"""CREATE MATERIALIZED VIEW IF NOT EXISTS prs.global_temperatures AS SELECT * FROM int.global_temperatures
"""
)

reset = (
    """DROP SCHEMA IF EXISTS raw CASCADE""",
    """DROP SCHEMA IF EXISTS int CASCADE""",
    """DROP SCHEMA IF EXISTS prs CASCADE"""
)

normalize_demographics = (
"""SET search_path TO int""",

"""ALTER TABLE int.racial_demographics
ADD COLUMN city_id INT NOT NULL DEFAULT 0""",

"""UPDATE int.racial_demographics
SET city_id = cities.city_id
FROM int.cities
WHERE racial_demographics.state_code = cities.state_code
  AND racial_demographics.city = cities.city""",

"""ALTER TABLE int.racial_demographics
DROP COLUMN IF EXISTS state_code,
DROP COLUMN IF EXISTS state_name""",

"""ALTER TABLE int.demographics
ADD COLUMN city_id INT NOT NULL DEFAULT 0""",

"""UPDATE int.demographics
SET city_id = cities.city_id
FROM int.cities
WHERE demographics.state_code = cities.state_code
  AND demographics.city = cities.city""",

"""ALTER TABLE int.demographics
DROP COLUMN IF EXISTS state_code,
DROP COLUMN IF EXISTS state_name,
DROP COLUMN IF EXISTS city"""
)

amend_intermediate_immigration = (
"""ALTER TABLE int.immigration
ADD COLUMN port_city VARCHAR DEFAULT NULL,
ADD COLUMN port_state_or_country VARCHAR DEFAULT NULL""",

"""UPDATE int.immigration AS imm
SET port_city = ports.city, port_state_or_country = ports.state
FROM raw.i94_port_codes AS ports
WHERE imm.port_code = ports.i94_port_code"""
)
