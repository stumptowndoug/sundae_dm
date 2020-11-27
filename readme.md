# Sundae Homework Assignment

Author: Doug Dement<br/>
[LinkedIn](https://www.linkedin.com/in/doug-dement-34795551/)<br/>
email: dougdmail@gmail.com



## Assigntment Questions:
 **1. Calculate response rate, appointment rate, contract rate in aggregate by County & City**
    * [County Response Rates](https://docs.google.com/spreadsheets/d/1sjsWsPg0gsrzWZHYHY8fLAMaPqkGoVkWORcCJ5MPFQ4/edit#gid=1307001298)
    * [City Response Rates](https://docs.google.com/spreadsheets/d/1sjsWsPg0gsrzWZHYHY8fLAMaPqkGoVkWORcCJ5MPFQ4/edit#gid=130315298)
    
 **2. How does square footage and year built impact the performance of direct mail, if at all?**
    * There is very little correlation between either square footage or year built within repsonse rates.
    * When filtering for outliers (square footage > 0, year_built > 1900) the averages are as follows:
       * Response square_feet AVG = 1,410
       * Non-Response square_feet AVG = 1,439
       * Response year_built AVG = 1961
       * Non-Response year_built = 1961
       
    * There is also no significant correlation using a correlation matrix
       * ![Correlation Matrix](https://sundae-homework.s3-us-west-2.amazonaws.com/sundae_4.png)


## Documentation:

### Step 1
Downloaded a CSV file with 528,549 rows related to a direct mail file.

### Step 2
When opening the file there we quoted characters around the following fields:
  * city
  * county
  * year_built
  
![Original CSV](https://sundae-homework.s3-us-west-2.amazonaws.com/sundae_1.png)
  

### Step 3
To alleviate data issues downstream, I saved a new file without quoted characters.

![Updated CSV](https://sundae-homework.s3-us-west-2.amazonaws.com/sundae_2.png)

### Step 4
For analysis I decided to upload the file to an AWS S3 bucket and used Athena to query the data.

```SQL
----------------------------------------------------------------
--CREATE mail_data TABLE IN ATHENA
----------------------------------------------------------------


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
```

### Step 5
I wanted to understand what duplicates were in the data file and I found the following:

* Count of property_id's: **528,549**
* Distinct property_id’s: **528,537**
* Delta: **12 duplicate records**

```SQL
----------------------------------------------------------------
-- QUERY DATA TO SEE HOW MANY DUPLCIATE PROPERTY_ID'S THERE ARE
----------------------------------------------------------------

SELECT
COUNT(property_id) as count,
COUNT(distinct property_id) as dist_count

FROM
sundae_dm.mail_data
```

### Step 6
Per the instructions, of those 12 duplicates we want to keep the record that has made it furthest along in the funnel.  In order to do that I wrote up a quick window function to rank order by contract, appointment, lead in descending order. Below is a query to check that the logic is working as intended.


```SQL
----------------------------------------------------------------
-- FIND DISTINCT PROPERTY_ID's FURTHEST ALONG IN PIPELINE
----------------------------------------------------------------

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
```

![Duplicate Logic Update](https://sundae-homework.s3-us-west-2.amazonaws.com/sundae_3.png)


### Step 7
Created a view with only distinct records rank ordered

```SQL
-------------------------------------------------------------------------------
-- CREATE VIEW TO ONLY INCLUDE DISTINCT PROPERTY_ID'S FURTHEST ALONG IN FUNNEL
-------------------------------------------------------------------------------

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
```

### Step 8
Query data for response rate, appointment rate, contract rate by city

```SQL
-------------------------------------------------------------------------------
-- QUERY DATA FOR RESPONSE RATES BY CITY
-------------------------------------------------------------------------------

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
```

### Step 9
Query data for response rate, appointment rate, contract rate by county

```SQL
-------------------------------------------------------------------------------
-- QUERY DATA FOR RESPONSE RATES BY COUNTY
-------------------------------------------------------------------------------

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
```

### Step 10
Repsonse rate analysis by sqare feet and year built

```SQL
-------------------------------------------------------------------------------
-- RESPONSE RATE ANALYSIS FOR SQUARE FEET & YEAR BUILT
-------------------------------------------------------------------------------

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
```

```PY

#---RESPONSE RATE ANALYSIS IN PYTHON---#

import pandas as pd
import seaborn as sn
import matplotlib.pyplot as plt

data = pd.read_csv('/Users/DougDement/Documents/Dev/sundae/final_sundae_data.csv')

df = pd.DataFrame(data,columns=['lead','appointment','contract','square_feet','year_built'])
df = df[df['square_feet'] > 0]
df = df[df['year_built'] > 1900]

df[:10]

df.corr(method ='pearson')

corrMatrix = df.corr()
sn.heatmap(corrMatrix, annot=True)
plt.show()
```
