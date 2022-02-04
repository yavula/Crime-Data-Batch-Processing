-- create database
create database crime_data;

-- use the above created database
use crime_data;

-- create the table required
create table crime_reported (id int not null,case_number varchar(50) not null,date_value varchar(70),block_value varchar(200),
iucr varchar(20),primary_type varchar(250),description_value varchar(200),local_description varchar(200),arrest varchar(10), 
domestic varchar(10), beat varchar(50),district varchar(50),ward int,community_area varchar(50), fbi_code varchar(50),
x_coordinate int, y_coordinate int, year int,updated_on varchar(70),latitude decimal(11,9),longitude decimal(11,9), location varchar(25))
;

-- load data present on the EC2 instance into the table created above
LOAD DATA LOCAL INFILE '/home/ec2-user/rows.csv' INTO TABLE crime_reported FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;