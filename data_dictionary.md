# Data Dictionary
Note that unless specified, the data dictionary covers the tables in the prs schema, as these are intended for wider 
use.

## airport_codes
A list of all airport codes and the attributes associated with each airport.

### Dictionary
- ident: Unique key for each row
- type: Type of airport
- name: Name of airport
- elevation_ft: Airport elevation above sea level in feet
- continent: Two letter abbreviation of continent airport is in
- iso_country: ISO country code
- iso_region: ISO region code (COUNTRY-STATE)
- municipality: Name of the municipality containing the airport
- gps_code: GPS code
- iata_code: IATA Code
- local_code: Local airport code
- latitude: Latitude of the airport
- longitude: Longitude of the airport

## demographics
Demographic data for cities in the United States

### Dictionary
- city: City name
- state_code: State code as two letter abbreviation
- state_name: Name of US state
- median_age: The median age from the city's population
- male_population: Number of male people in the city
- female_population: Number of female people in the city
- number_of_veterans: Number of veterans in the city
- foreign_born: Number of people that were born in other countries
- average_household_size: Average number of people that live in a household
- race: Race label for column 'count'
- count: Number of people of the race in column race

## Temperatures Data
Dataset of global temperatures. 

### Dictionary
- record_date: Date temperature reading was taken
- average_temperature: Global average land temperature in celsius
- average_temperature_uncertainty: The 95% confidence interval around the average
- country: Name of the country where temperature reading was taken

## Immigration
Records of immigrants to the United States. Each record represents an admission of one person.

### Dictionary
- cicid: Unique label for specific record of immigration
- year: Year immigration record took place
- month: Month immigration record took place
- citizenship_country: Country the immigrant is a citizen of
- residency_country: Country the immigrant is a resident of
- port_code: I94 code identifying the port where immigration record occured
- arrival_date: Arrival Date to the USA
- arrival_mode: Arrival Mode (Air, Sea, Land, Not reported)
- state: Full name of state where port is located
- departure_date: Date immigrant departed from the USA
- age_in_years: Age of immigrant in years
- visa_purpose: Visa classification (Business, Pleasure, Student)
- date_added_to_files: Date record was added to I-94 files
- visapost: Department of State where Visa was issued
- occupation: Occupation that immigrant will perform in U.S.
- arrival_flag: Admitted or paroled into the U.S.
- departure_flag: Departed, lost I-94 or is deceased
- update_flag: Either apprehended, overstayed, adjusted to perm residence
- match_flag: Match of arrival and departure records
- birth_year: Year immigrant was born
- admitted_until: Date immigrant is permitted to stay until
- gender: Single character gender flag M(ale) F(emale)
- insnum: INS number
- airline: Airline used to arrive in the U.S.
- admnum: Admission Number
- flight_number: Flight number of Airline used to arrive in U.S.
- visa_type: Class of admission legally admitting the non-immigrant to temporarily stay in U.S.
- port_city: Name of city admission port is located in
- port_state_or_country: Name of US state or country the admission port is located in