# Run instructions

* Can be run from IDE only at that point
* No config support (HOCON)
* For IDE runs files are placed in classpath resources (not included) [green_tripdata_2023-10.parquet, yellow_tripdata_2023-10.parquet, green_tripdata_2023-11.parquet, yellow_tripdata_2023-11.parquet]
* When run from IDE do not forget to **add all dependencies with "provided" scope to classpath**

You must copy mentioned files into following directories
* src/main/resources/data/nyc/taxi/10/green_tripdata_2023-10.parquet
* src/main/resources/data/nyc/taxi/10/yellow_tripdata_2023-10.parquet
* src/main/resources/data/nyc/taxi/11/green_tripdata_2023-11.parquet
* src/main/resources/data/nyc/taxi/11/yellow_tripdata_2023-11.parquet

This may require to update [ConfigurationParams.java](main%2Fjava%2Fai%2Fcognitiv%2Fnyc%2Ftaxi%2Freader%2Fcognitiv%2Fconfig%2FConfigurationParams.java)
The above file internals arrays are fallback if no path provided in params (done for testing or if we would like to support any specific dedicated directory)

# Submit spark application [IN PROGRESS]
* Run docker-compose up - it should start spark cluster of a master and 2 workers
