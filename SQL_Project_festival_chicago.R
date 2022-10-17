library(tidyverse)
library(RPostgres)

#CREATE THE DATABASE----
#_create a connection to project database----
con <- dbConnect(
  drv = dbDriver('Postgres'), 
  dbname = 'fest8',
  host = 'db-postgresql-nyc1-44203-do-user-8018943-0.b.db.ondigitalocean.com', 
  port = 25060,
  user = 'proj22b_8', 
  password = 'AVNS_4JKVJgNCqiM4cVZf-Hu'
)

#_create locations table----

stmt <- 'CREATE TABLE locations (
          iocn_id int,
          lat decimal(7,5),
          lng decimal(7,5),
          data_type varchar(20),
          name varchar(100),
          address varchar(50),
          city varchar(50),
          st char(2),
          zip char(5),
          PRIMARY KEY(iocn_id)
        );'
dbExecute(con, stmt)  

#_create venues table----

stmt <- 'CREATE TABLE venues (
          venue_id int,
          venue_name varchar(50),
          iocn_id int,
          PRIMARY KEY(venue_id),
          FOREIGN KEY(iocn_id) REFERENCES locations
        );'
dbExecute(con, stmt)  


#_create resturants table----

stmt <- 'CREATE TABLE resturants (
          rest_id int,
          rest_name varchar(100),
          rest_type varchar(50),
          rest_phone varchar(11),
          rest_website varchar(200),
          rest_open_time time,
          rest_close_time time,
          price_range varchar(10),
          rating decimal(2,1),
          iocn_id int,
          PRIMARY KEY(rest_id),
          FOREIGN KEY(iocn_id) REFERENCES locations
        );'
dbExecute(con, stmt)  



#_create attractions table----

stmt <- 'CREATE TABLE attractions (
          attract_id int,
          attract_name varchar(100),
          attract_rating decimal(2,1),
          attract_price varchar(10),
          distance_from_parks_miles decimal(2,1),
          attract_open_time varchar(10),
          attract_close_time varchar(10),
          iocn_id int,
          PRIMARY KEY(attract_id),
          FOREIGN KEY(iocn_id) REFERENCES locations
        );'
dbExecute(con, stmt)  


#_create hotels table----

stmt <- 'CREATE TABLE hotels (
          hot_id int,
          hot_name varchar(100),
          hot_rating decimal(2,1),
          hot_phone char(11),
          hot_checkin_time time,
          hot_checkout_time time,
          hot_website varchar(200),
          iocn_id int,
          PRIMARY KEY(hot_id),
          FOREIGN KEY(iocn_id) REFERENCES locations
        );'
dbExecute(con, stmt) 


#_create rooms table----

stmt <- 'CREATE TABLE rooms (
          room_id int,
          price_per_night decimal(3,0),
          room_size_sqft decimal(3,0),
          bed_type varchar(10),
          hot_id int,
          PRIMARY KEY(room_id),
          FOREIGN KEY(hot_id) REFERENCES hotels
        );'
dbExecute(con, stmt) 


#_create artists table----

stmt <- 'CREATE TABLE arts (
          art_id int,
          art_name varchar(50),
          genre varchar(20),
          vid varchar(11),
          perform_day int,
          PRIMARY KEY(art_id)
);'
dbExecute(con, stmt) 


#_create songs table----

stmt <- 'CREATE TABLE songs (
          song_id int,
          song_name varchar(100),
          genre varchar(20),
          dor date,
          yt_views bigint,
          art_id int,
          PRIMARY KEY(song_id),
          FOREIGN KEY(art_id) REFERENCES arts
);'
dbExecute(con, stmt) 


#******************----
#EXTRACT----
#_read source data----
setwd('C:\\Users\\Tysonzhou\\Desktop\\Columbia U\\summer\\5310 SQL\\Group project')
df = read.csv('rooms.csv')
df = read.csv('hotels.csv')
df = read.csv('locations.csv')
df = read.csv('attractions.csv')
df = read.csv('restaurants.csv')
df = read_csv('venues.csv')
df = read_csv('songs.csv')
art = read_csv('artists.csv')


# ****************************----
# LOAD----


# _load venues table----
dbWriteTable(
  conn = con,
  name = 'venues',
  value = df,
  row.names = FALSE,
  append = TRUE
)


# _load rooms table----
dbWriteTable(
  conn = con,
  name = 'rooms',
  value = df,
  row.names = FALSE,
  append = TRUE
)


# _load hotels table----
dbWriteTable(
  conn = con,
  name = 'hotels',
  value = df,
  row.names = FALSE,
  append = TRUE
)



# _load locations table----
dbWriteTable(
  conn = con,
  name = 'locations',
  value = df,
  row.names = FALSE,
  append = TRUE
)



# _load attractions table----
dbWriteTable(
  conn = con,
  name = 'attractions',
  value = df,
  row.names = FALSE,
  append = TRUE
)

venues


# _load resturants table----
dbWriteTable(
  conn = con,
  name = 'resturants',
  value = df,
  row.names = FALSE,
  append = TRUE
)


#_load arts table----
dbWriteTable(
  conn = con,
  name = 'arts',
  value = art,
  row.names = FALSE,
  append = TRUE)



#_load songs table----
dbWriteTable(
  conn = con,
  name = 'songs',
  value = df,
  row.names = FALSE,
  append = TRUE)


#******************----
#END----