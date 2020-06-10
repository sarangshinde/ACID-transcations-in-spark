How to run pipeline part.

Create new or replace paths in application.local.conf. 

kafka Consumer sections needs kafka and way to populate data.
Here we have used setup provided by Hudi Team.

https://hudi.apache.org/docs/docker_demo.html

Follow above link and perform setup. 
After this we need to change kafka and zookeeper version in existing setup.
For that go to /docker/compose/docker-compose_hadoop284_hive233_spark244.yml change version to following  

bitnami/kafka:2.5.0 

bitnami/zookeeper:3.6.1

To generate data using script specified in above link.

To run pipeline:
1. insert first batch to kafka.
2. consume data using kafka consumer, then run silver layer and gold.
3. For incremental loading comment some of the dumpexisting code section.
