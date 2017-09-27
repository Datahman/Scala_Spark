
EX 1
----

# Log on Hive
beeline -u jdbc:hive2://
--------------------------------------------------------------------------------
# Create team database
--------------------
CREATE DATABASE fr4;

USE fr4;

---------------------------------------------------------------------------------
# Make directory on Hadoop

hadoop fs -mkdir -p /Team_Project_WK7/FR4/EX1
---------------------------------------------------------------------------------
# Copy file to Hadoop

hadoop fs -copyFromLocal u.user /Team_Project_WK7/FR4/EX1
---------------------------------------------------------------------------------
# Create an External table and fill in the schema

CREATE EXTERNAL TABLE  if not exists USERS_INFO (
Id bigint,
Age bigint,
Gender string,
Occupation string,
Zip_Code bigint
)
comment "Imported user info to Database FR4 from u.users file"
row format delimited fields terminated by '|'
stored as textfile
location '/Team_Project_WK7/FR4/EX1/';
---------------------------------------------------------------------------------


# Quereies:

# SHOW TABLE

SELECT * FROM USERS_INFO;
---------------------------------------------------------------------------------

# COUNT NUMBER OF USER ENTRIES

SELECT COUNT(*) FROM USERS_INFO;
---------------------------------------------------------------------------------

# FIND COUNT OF PEOPLE FULFILLINg TWO CONDITIONS

SELECT COUNT(*) FROM USERS_INFO u WHERE u.Occupation = "student" AND  u.Gender == "M";
---------------------------------------------------------------------------------
# FIND MEAN AGE BY OCCUPATION

SELECT u.Occupation, AVG(u.Age) FROM USERS_INFO u  GROUP BY u.Occupation;

---------------------------------------------------------------------------------
# FIND COUNT OF PEOPLE PER OCCUPATION

SELECT u.Occupation, COUNT(u.Id) FROM USERS_INFO u  GROUP BY u.Occupation;

---------------------------------------------------------------------------------

---
EX2
---
# MAKE INTERNAL REPOS (WITHIN HIVE)
---------------------------------------------------------------------------------

CREATE TABLE ITEMS (
Film_Id bigint,
Film_Title_Year string,
Release_Date string,
Video_Release_Date string,
IMDB_Link string,
Unknown tinyint,
Action tinyint,
Adventure tinyint,
Animation tinyint,
Childrens tinyint,
Comedy tinyint,
Crime tinyint,
Documentary tinyint,
Drama tinyint,
Fantasy tinyint,
Film_Noir tinyint,
Horror tinyint,
Musical tinyint,
Mystery tinyint,
Romance tinyint,
Sci_Fi tinyint,
Thriller tinyint,
War tinyint,
Western tinyint
)
row format delimited fields terminated by '|'
stored as textfile;


-------------------------------------------------------
# SEND FILE TO HIVE I.E @ /usr/hive/warehouse/items/u.item 

hadoop fs -put u.item /user/hive/warehouse/items

--------------------------------------------------------


# Load data (Internal) to the table

LOAD DATA  INPATH '/user/hive/warehouse/items/u.item' OVERWRITE INTO TABLE ITEMS;

-------------------------------------------------------------------------------

# SHOW TABLE CONTENTS 

SELECT * FROM ITEMS;
-------------------------------------------------------------------------------

# List total film count as per genre

SELECT COUNT(*) AS Action_Films_Count FROM ITEMS WHERE action=1;

# List all the films released between the period 1920 and 1990.

SELECT  film_title_year as Films_Released_Between_1920_1990 FROM ITEMS WHERE substr(release_date,8) > 1920 AND substr(release_date,8) <= 1990 ;

# List film titles per Alphabet

SELECT film_title_year as Film_Title_per_Alphabet FROM ITEMS WHERE film_title_year LIKE concat('T%');

## EX 2 ## 
## Create an internal table ITEMS using data  copied to IMPALA from HDFS ##

## IMPALA shell ##

# In fr4 DATABASE make table ITEMS 

USE fr4;

CREATE TABLE ITEMS
(
Film_Id bigint,
Film_Title_Year string,
Release_Date string,
Video_Release_Date string,
IMDB_Link string,
Unknown tinyint,
Action tinyint,
Adventure tinyint,
Animation tinyint,
Childrens tinyint,
Comedy tinyint,
Crime tinyint,
Documentary tinyint,
Drama tinyint,
Fantasy tinyint,
Film_Noir tinyint,
Horror tinyint,
Musical tinyint,
Mystery tinyint,
Romance tinyint,
Sci_Fi tinyint,
Thriller tinyint,
War tinyint,
Western tinyint
)
row format delimited fields terminated by '|'
stored as textfile;

# Check  table fields
DESCRIBE ITEMS;

# Supply data to Impala
LOAD DATA INPATH '/user/cloudera/fr4/items/u.item' INTO TABLE ITEMS;  

# Verify data file removed from HDFS
hadoop fs -cat /user/cloudera/fr4/items/u.item

# SHOW TABLE CONTENT
SELECT * FROM ITEMS;

## QUERIES ##

# List total film count as per genre

SELECT COUNT(*) AS Action_Films_Count FROM ITEMS WHERE action=1;

# List all the films released between the period 1920 and 1990.

SELECT  film_title_year as Films_Released_Between_1920_1990 FROM ITEMS WHERE cast(substr(release_date,8)as int) > 1920 AND cast(substr(release_date,8) as int) <= 1990 ;


# List film titles per Alphabet

SELECT film_title_year as Film_Title_per_Alphabet FROM ITEMS WHERE film_title_year LIKE concat('T%');





# EX3
-- CREATE DATABASE

CREATE DATABASE MOVIE;

USE MOVIE;

CREATE EXTERNAL TABLE IF NOT EXISTS MOVIE_TAGS (
userId bigint,
movieId bigint,
tag string,
timestamp bigint
)
comment "Import movie tags data"
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/cloudera/moviedb/tags';



CREATE EXTERNAL TABLE IF NOT EXISTS MOVIE_RATINGS (
userId bigint,
movieId bigint,
rating float,
timestamp bigint
)
comment "Import movie ratings data"
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/cloudera/moviedb/rating';

-- Queries
-- Show rating frequency given by each user ID

SELECT r.userid,count(r.rating) as Frequency_Rating_per_User FROM movie_ratings r join movie_tags t on t.userid = r.userid and t.movieid = r.movieid GROUP BY r.userid;



---------- Additional ex1S


CREATE EXTERNAL TABLE IF NOT EXISTS MOVIE_TAGS 
(user_id bigint, movie_id bigint,rating string, timestamp bigint)
 comment "table to store information about textual movie ratings" 
 ow format delimited fields terminated by ',' stored as textfile location '/movietags/';


create external table if not exists movierating (user_id bigint, movie_id bigint, rating float, time bigint) comment "table to store movie numeric ratings" row format delimited fields terminated by ',' stored as textfile location '/movierating';

--Good
select explode(ngrams(sentences(lower(t.rating)),2,5)) as good_bigrams from movietags t inner join movierating r on t.movie_id = r.movie_id and t.user_id = r.user_id where r.rating >= 4; 
select explode(ngrams(sentences(lower(t.rating)),3,5)) as good_trigrams from movietags t inner join movierating r on t.movie_id = r.movie_id and t.user_id = r.user_id where r.rating >= 4; 
select explode(ngrams(sentences(lower(t.rating)),4,5)) as good_4grams from movietags t inner join movierating r on t.movie_id = r.movie_id and t.user_id = r.user_id where r.rating >= 4; 
--bad
select explode(ngrams(sentences(lower(t.rating)),2,5)) as bad_bigrams from movietags t inner join movierating r on t.movie_id = r.movie_id and t.user_id = r.user_id where r.rating <= 1;
select explode(ngrams(sentences(lower(t.rating)),3,5)) as bad_trigrams from movietags t inner join movierating r on t.movie_id = r.movie_id and t.user_id = r.user_id where r.rating <= 1;
select explode(ngrams(sentences(lower(t.rating)),4,5)) as bad_4grams from movietags t inner join movierating r on t.movie_id = r.movie_id and t.user_id = r.user_id where r.rating <= 1;



select explode(ngrams(sentences(lower(rating)),2,5)) as bigrams from movietags;
select explode(ngrams(sentences(lower(rating)),3,5)) as trigrams from movietags;
select explode(ngrams(sentences(lower(rating)),4,5)) as 4grams from movietags;





-- Task 2

show tables;


create table items(id bigint, Title string ,Date string, Video_release_date string,IMDb_URL string, unknown tinyint, Action tinyint, Adventure tinyint, Animation tinyint, Children tinyint, Comedy tinyint, Crime tinyint, Documentary tinyint, Drama tinyint, Fantasy tinyint, Film_Noir tinyint, Horror tinyint, Musical tinyint, Mystery tinyint, Romance tinyint, Sci_Fi tinyint, Thriller tinyint , War tinyint, Western tinyint) comment "table to store for users" row format delimited fields terminated by '|' stored as textfile


load data inpath '/user/hive/warehouse/movies/u.item' overwrite  into table items; 


select Title as  Drama_and_Action_Films from items where Action = 1 and Drama = 1;


select Title as Action_Films from items where Action = 1 limit 20;


select Title as Action_Films from items where Action = 1 order by "Title" limit 10 ;