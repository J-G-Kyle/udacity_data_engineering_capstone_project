set_up_postgres = ("""CREATE SCHEMA IF NOT EXISTS raw""",
"""SET search_path TO raw""",
"""
CREATE TABLE IF NOT EXISTS raw.immigration(
cicid       FLOAT,
i94yr       FLOAT,
i94mon      FLOAT,
i94cit      FLOAT,
i94res      FLOAT,
i94port     CHAR(3),
arrdate     FLOAT,
i94mode     FLOAT,
i94addr     CHAR(2),
depdate     FLOAT,
i94bir      FLOAT,
i94visa     FLOAT,
count       FLOAT,
dtadfile    CHAR(8),
visapost    CHAR(3),
occup       CHAR(3),
entdepa     CHAR(1),
entdepd     CHAR(1),
entdepu     CHAR(1),
matflag     CHAR(1),
biryear     FLOAT,
dtaddto     CHAR(10),
gender      CHAR(1),
insnum      CHAR(30),
airline     CHAR(3),
admnum      FLOAT,
fltno       CHAR(5),
visatype    CHAR(3)
)
""",
            
"""CREATE TABLE IF NOT EXISTS raw.demographics(
city                      CHAR(100),
state                     CHAR(100),
median_age                FLOAT,
male_population           INTEGER,
female_population         INTEGER,
total_population          INTEGER,
number_of_veterans        INTEGER,
foreign_born              CHAR(100),
average_household_size    CHAR(100),
state_code                CHAR(100),
race                      CHAR(100),
count                     CHAR(100)
)
""",

"""CREATE TABLE IF NOT EXISTS raw.airport_codes(
ident            CHAR(100),
type             CHAR(100),
name             CHAR(130),
elevation_ft     CHAR(100),
continent        CHAR(100),
iso_country      CHAR(100),
iso_region       CHAR(100),
municipality     CHAR(100),
gps_code         CHAR(100),
iata_code        CHAR(100),
local_code       CHAR(100),
coordinates      CHAR(100)
)
""",

"""CREATE TABLE IF NOT EXISTS raw.i94_country_codes(
i94code         INT,
country         CHAR(100)
)
""",

"""CREATE TABLE IF NOT EXISTS raw.i94_port_codes(
i94_port_code       CHAR(3),
city                CHAR(100),
state               CHAR(100)
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
CREATE TABLE IF NOT EXISTS immigration(
cicid               INT PRIMARY KEY,
year                INT,
month               INT,
citizenship_country CHAR(100),
residency_country   CHAR(100),
i94port_code        CHAR(3),
port_city           CHAR(100),
port_state          CHAR(100),
arrival_date        DATE,
arrival_mode        CHAR(100),
state               CHAR(30),
departure_date      DATE,
age_in_years        INT,
visa_purpose        CHAR(20),
date_added_to_files DATE,
visapost            CHAR(3),
occupation          CHAR(3),
arrival_flag        CHAR(1),
departure_flag      CHAR(1),
update_flag         CHAR(1),
match_flag          CHAR(1),
birth_year          INT,
admitted_until      DATE,
gender              CHAR(1),
insnum              CHAR(1),
airline             CHAR(3),
admnum              BIGINT,
flight_number       CHAR(5),
visa_type           CHAR(3)
)
""",

"""CREATE TABLE IF NOT EXISTS demographics(
city                      CHAR(100),
state_name                CHAR(100),
median_age                NUMERIC(3,1),
male_population           INT,
female_population         INT,
total_population          INT,
number_of_veterans        INT,
foreign_born              INT,
average_household_size    NUMERIC(3,2),
state_code                CHAR(100)
)
""",

"""CREATE TABLE IF NOT EXISTS cities(
city_id    SERIAL PRIMARY KEY,
city       CHAR(100),
state_code CHAR(2),
state_name CHAR(100)
)
""",

"""CREATE TABLE IF NOT EXISTS racial_demographics(
city       CHAR(100),
state_code CHAR(2),
state_name CHAR(100),
race       CHAR(100),
"count"    INT
)
""",

"""CREATE TABLE IF NOT EXISTS airport_codes(
ident            CHAR(100),
type             CHAR(100),
name             CHAR(130),
elevation_ft     INT,
continent        CHAR(100),
iso_country      CHAR(100),
iso_region       CHAR(100),
municipality     CHAR(100),
gps_code         CHAR(100),
iata_code        CHAR(100),
local_code       CHAR(100),
latitude         FLOAT,
longitude        FLOAT
)
"""

)

reset = (
    """DROP SCHEMA IF EXISTS raw CASCADE""",
    """DROP SCHEMA IF EXISTS int CASCADE"""
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
WHERE imm.port_code = ports.i94_port_code""",

"""ALTER TABLE int.immigration
DROP COLUMN port_code"""
)