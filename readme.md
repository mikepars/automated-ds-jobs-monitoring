<center>

![img](https://cdn-images-1.medium.com/fit/t/1600/480/1*mf619XEdHd1O2MlyhEKcig.gif)

# Automated ETL & Monitoring for DS Jobs in Glassdoor
### Using Airflow and Elasticsearch

</center>

### Objective

In this project, we want to create a system to automate ETL task, and monitor real time data inside the database using Kibana. The data we'll be using is Data Science job posts from Glassdoor (the dataset itself was from [Kaggle](https://www.kaggle.com/datasets/rashikrahmanpritom/data-science-job-posting-on-glassdoor/data?select=Uncleaned_DS_jobs.csv).

Once the data that we gathered were cleaned and transformed on Airflow, then it is loaded to database, and uploaded to Elasticsearch for monitoring through Kibana.