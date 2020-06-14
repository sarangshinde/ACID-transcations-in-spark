#Deployment Script
sbt clean universal:packageBin

#Go to mount path on adhoc-2 continer where zip file is created
unzip transcationswithspark-0.1.0-SNAPSHOT.zip
cd transcationswithspark-0.1.0-SNAPSHOT;
commaSepratedLibs=$(ls -1p lib/*.jar | xargs echo | sed 's/ /,/g')
echo ${commaSepratedLibs}


#load data and run pipeline

#load batch 1 from local machine
cat docker/demo/data/batch_1.json | kafkacat -b kafkabroker -t stock_ticks -P

#Go to adhoc-2 container and run following commands
spark-submit --master local  --name consumer --files conf/application.local.conf,conf/application.container.conf --jars ${commaSepratedLibs} --class datapipeline.consumer.KafkaConsumer lib/transcationswithspark.transcationswithspark-0.1.0-SNAPSHOT.jar conf/application.container.conf
#stop consumer once data is loaded into hdfs

#Query Consumer Data
spark-submit --master local  --name consumer --files conf/application.local.conf,conf/application.container.conf --jars ${commaSepratedLibs} --class datapipeline.consumer.QueryData lib/transcationswithspark.transcationswithspark-0.1.0-SNAPSHOT.jar conf/application.container.conf


spark-submit --master local  --name silverlayer --files conf/application.local.conf,conf/application.container.conf --jars ${commaSepratedLibs} --class datapipeline.cdmlayers.silverlayer.SilverLayerBuilder lib/transcationswithspark.transcationswithspark-0.1.0-SNAPSHOT.jar conf/application.container.conf ""

spark-submit --master local  --name goldlayer --files conf/application.local.conf,conf/application.container.conf --jars ${commaSepratedLibs} --class datapipeline.cdmlayers.goldlayer.GoldLayerBuilder lib/transcationswithspark.transcationswithspark-0.1.0-SNAPSHOT.jar conf/application.container.conf ""

#load batch 2 from local machine
cat docker/demo/data/batch_2.json | kafkacat -b kafkabroker -t stock_ticks -P

#Go to adhoc-2 container and run following commands
spark-submit --master local  --name consumer --files conf/application.local.conf,conf/application.container.conf --jars ${commaSepratedLibs} --class datapipeline.consumer.KafkaConsumer lib/transcationswithspark.transcationswithspark-0.1.0-SNAPSHOT.jar conf/application.container.conf
#stop consumer once data is loaded into hdfs

#Query Consumer Data
spark-submit --master local  --name consumer --files conf/application.local.conf,conf/application.container.conf --jars ${commaSepratedLibs} --class datapipeline.consumer.QueryData lib/transcationswithspark.transcationswithspark-0.1.0-SNAPSHOT.jar conf/application.container.conf


spark-submit --master local  --name silverlayer --files conf/application.local.conf,conf/application.container.conf --jars ${commaSepratedLibs} --class datapipeline.cdmlayers.silverlayer.SilverLayerBuilder lib/transcationswithspark.transcationswithspark-0.1.0-SNAPSHOT.jar conf/application.container.conf incremental

spark-submit --master local  --name goldlayer --files conf/application.local.conf,conf/application.container.conf --jars ${commaSepratedLibs} --class datapipeline.cdmlayers.goldlayer.GoldLayerBuilder lib/transcationswithspark.transcationswithspark-0.1.0-SNAPSHOT.jar conf/application.container.conf incremental

##Reset state

#Go to kafkabroker container and delete topic data
 kafka-topics.sh --zookeeper zookeeper:2181 --delete --topic stock_ticks

#Go to adhoc-2 container and delete all hdfs data
hadoop fs -rmr /user/raw_delta_checkpoint
hadoop fs -rmr  /user/hive/warehouse/raw_data_delta
hadoop fs -rmr  /user/hive/warehouse/silver_data_delta
hadoop fs -rmr /user/hive/warehouse/gold_data_hudi

# Drop gold table from hive

