# Introduction
This is a project build on [Apache Spark](https://spark.apache.org). The goal is to build an templated and reusable application that enable doing ETL easily without coding more.
The main idea includes the following steps:
- Firstly, loading data from the common supported sources and assigning them as temp view.
- Leveraging the spark SQL API to do transformation and join dataframe and assigning all new created dataframes as temp view. 
- Storing temp view to the popular supported sinks.
- Using the yaml file to define the application's flow (read, query, write or show).
