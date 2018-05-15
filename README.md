## How to run the application

* We can run the application using the below command
* The three elements which are followed by the JAR name(CIA-Materialization.jar) are program arguments. 

```
cd /Users/bguru1/balamurugan/My_Workspace/cia_material

spark-submit --class "com.poc.sample.IncrementalRunner" --master local[4]  --conf spark.configFileLocation=/Users/bguru1/balamurugan/Datasets/materialization-config.json --conf spark.es.nodes=localhost --conf spark.es.port=9200 CIA-Materialization.jar