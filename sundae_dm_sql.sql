
#---CREATE TABLE FOR SUNDAE MAIL DATA---#


CREATE EXTERNAL TABLE IF NOT EXISTS sundae_dm.mail_data (
  `property_id` bigint,
  `city` string,
  `county` string,
  `lead` smallint,
  `appointment` smallint,
  `contract` smallint,
  `square_feet` int,
  `year_built` int 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://sundae-dm/'
TBLPROPERTIES ('has_encrypted_data'='false',
  "skip.header.line.count"="1");


#---QUERY DATA TO SEE HOW MANY DUPLCIATE PROPERTY_ID'S THERE ARE---#


SELECT
COUNT(property_id) as count,
COUNT(distinct property_id) as dist_count

FROM
sundae_dm.mail_data


#---FIND DISTINCT PROPERTY_ID FURTHEST ALONG IN PIPELINE---#


SELECT
md.*,
row_number() OVER (PARTITION BY md.property_id ORDER BY md.contract DESC, md.appointment DESC, md.lead DESC) as rank_order

FROM
sundae_dm.mail_data md
INNER JOIN
(
SELECT
property_id,
COUNT(property_id)

FROM
sundae_dm.mail_data

GROUP BY
1

HAVING
count(property_id) > 1
) AS dup ON md.property_id = dup.property_id


#---CREATE VIEW TO ONLY INCLUDE DISTINCT PROPERTY_ID'S FURTHEST ALONG IN FUNNEL---#


CREATE OR REPLACE VIEW dist_ordered_mail_data 
AS

SELECT
a.property_id,
lower(a.city) as city,
lower(a.county) as county,
a.lead,
a.appointment,
a.contract,
a.square_feet,
a.year_built,
a.rank_order

FROM
(
SELECT
md.*,
row_number() OVER (PARTITION BY md.property_id ORDER BY md.contract DESC, md.appointment DESC, md.lead DESC) as rank_order

FROM
sundae_dm.mail_data md
) AS a

WHERE
rank_order = 1


#---DOUBLE CHECK THAT RECORD COUNTS COME OUT TO 528,537 AS EXPECTED---#


SELECT
COUNT(property_id) as count,
COUNT(DISTINCT property_id) as dist_count

FROM
dist_ordered_mail_data


#---QUERY DATA FOR RESPONSE RATES BY CITY---#

SELECT
county,
city,
COUNT(property_id) as mail_count,
SUM(lead) as lead_count,
SUM(appointment) as appointment_count,
SUM(contract) as contract_count,
CASE
  WHEN SUM(lead) = 0
  THEN 0.0
  ELSE (1.0 * SUM(lead)) / COUNT(property_id) 
END AS response_rate,
CASE
  WHEN SUM(appointment) = 0
  THEN 0.0
  ELSE (1.0 * SUM(appointment)) / SUM(lead)
END AS appointment_rate,
CASE
  WHEN SUM(contract) = 0
  THEN 0.0
  ELSE (1.0 * SUM(contract)) / SUM(appointment)
END AS contract_rate

FROM
dist_ordered_mail_data

GROUP BY
1,2

ORDER BY
county,
city


#---QUERY DATA FOR RESPONSE RATES BY COUNTY---#

SELECT
county,
COUNT(property_id) as mail_count,
SUM(lead) as lead_count,
SUM(appointment) as appointment_count,
SUM(contract) as contract_count,
CASE
  WHEN SUM(lead) = 0
  THEN 0.0
  ELSE (1.0 * SUM(lead)) / COUNT(property_id) 
END AS response_rate,
CASE
  WHEN SUM(appointment) = 0
  THEN 0.0
  ELSE (1.0 * SUM(appointment)) / SUM(lead)
END AS appointment_rate,
CASE
  WHEN SUM(contract) = 0
  THEN 0.0
  ELSE (1.0 * SUM(contract)) / SUM(appointment)
END AS contract_rate

FROM
dist_ordered_mail_data

GROUP BY
1

ORDER BY
county


#---QUERY DATA FOR RESPONSE RATE BY SQUARE FEET & YEAR BUILT---#

SELECT
lead,
AVG(square_feet) as avg_sq_ft,
MIN(square_feet) as min_sq_ft,
MAX(square_feet) as max_sq_ft,

AVG(year_built) as avg_yr_built,
MIN(year_built) as min_yr_built,
MAX(year_built) as max_yr_built

FROM
dist_ordered_mail_data

WHERE
square_feet > 0
and year_built > 1900

GROUP BY
1














