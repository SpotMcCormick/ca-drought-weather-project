# California Drought & Weather Correlation Tool

## About the project
Welcome to my other project of weather and drought data. I am going back to my home state of CA and using data sources.  
I wanted to push my data engineering skills, so I decided to go crazy with the most popular tools in the data stack.  
With my recent study of data lake architecture I decided to make a data lakehouse. I thought it was the perfect opportunity to try out these skills I have leared. 

The project is still a work in progress so be sure to keep checking back in. I will have updates below. 

I am using medallion architecture so we have a bronze layer (stores all raw data as is), a silver layer (treating this like a staging area), and a gold layer(our semantic model).
 ### Bronze Layer
First off I am extracting raw data through python via API's and storing that data in an aws S3 bucket. 

Data sources:   
https://droughtmonitor.unl.edu/DmData/DataDownload/WebServiceInfo.aspx  
https://data.ca.gov/dataset/ca-geographic-boundaries

The only data I am not using from an API is the meteostat library  
https://dev.meteostat.net/

### Silver Layer

After the data is extracted i then extract it again from the s3 bucket do minimal transformations and turn it into an iceberg table format

### Gold Layer
After all the data is transformed and staged in the silver layer I use DBT to make an analytical view for our gold sematic layer with an MVP of a tableau dashboard.  
**Dashboard Link**:
https://public.tableau.com/app/profile/jeremy.mccormick/viz/CACountyDroughtAndWeatherCorrelationTool/Dashboard#1

All in all this has been a great project to learn, grow, and sharpen my skills as a data professional. 

**Tools used**  
Python  
Pyspark  
Airflow  
DBT  
AWS Glue  
AWS Athena  
Apache Iceberg  

## Updates 
11/18/2025 - Started Github for project  
12/14/2025 - All initial load scripts ran and in s3  
12/15/2025 - tested initial load with dbt to make gold layer  
12/20/2025 - started incremental load scripts  
1/2/2025 - tested an incremental load script with airflow  
1/8/2026 - build dashboard off initial load data  