# California Drought & Weather Correlation Tool

## About the project

I'm a data engineer and analyst — currently contracted at a startup and working full time at a utility company where I build pipelines and dashboards day to day. I built this project to push myself into the AWS ecosystem and get hands on with the tools I kept seeing in job descriptions.

I went with a full data lakehouse setup because I had been studying medallion architecture and wanted to actually build one instead of just reading about it. The domain made sense to me — CA drought data is publicly available, interesting, and has real stakes.

The project is still a work in progress so keep checking back. Updates are at the bottom.

**Dashboard**: [CA County Drought & Weather Correlation Tool](https://public.tableau.com/app/profile/jeremy.mccormick/viz/CACountyDroughtAndWeatherCorrelationTool/Dashboard#1)

---

## How it works

Three data sources feed into a bronze → silver → gold pipeline:

**Bronze** — raw data lands in S3 as-is. No transformation, just storage.

Data sources:  
https://droughtmonitor.unl.edu/DmData/DataDownload/WebServiceInfo.aspx  
https://data.ca.gov/dataset/ca-geographic-boundaries  
https://dev.meteostat.net/ (python library, not an API)

**Silver** — data gets pulled from bronze, lightly cleaned and typed, then written into Apache Iceberg tables backed by S3 and cataloged in AWS Glue. Iceberg gives me schema enforcement, ACID transactions, and time travel which is nice to have for environmental data that changes week to week.

**Gold** — DBT models running against Athena join everything together into something a BI tool can actually use. This is what powers the Tableau dashboard.

---

## Infrastructure

I self hosted the entire Airflow setup on my home server running in Docker Compose — webserver, scheduler, and Postgres metadata DB all containerized. No managed service, just a box in my house running pipelines 24/7. Three DAGs handle the scheduling for each data source:

| DAG | Schedule |
|---|---|
| `gis_monthly` | 1st of every month |
| `drought_monitor_friday` | Every Friday |
| `meteostat_daily` | Every day |

---

## Tools used

Python · PySpark · Apache Airflow · DBT · AWS S3 · AWS Glue · AWS Athena · Apache Iceberg · Docker · Tableau

---

## Updates

11/18/2025 - Started Github for project  
12/14/2025 - All initial load scripts ran and in S3  
12/15/2025 - Tested initial load with DBT to make gold layer  
12/20/2025 - Started incremental load scripts  
1/2/2026 - Tested an incremental load script with Airflow  
1/8/2026 - Built dashboard off initial load data  
4/17/2026 - Dockerized Airflow and deployed to home server, all 3 incremental DAGs running in production