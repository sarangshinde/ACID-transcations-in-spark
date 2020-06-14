#Deployment Script
sbt clean universal:packageBin

#Go to mount path on continer where zip file is created
unzip transcationswithspark-0.1.0-SNAPSHOT.zip
cd transcationswithspark-0.1.0-SNAPSHOT;
commaSepratedLibs=$(ls -1p lib/*.jar | xargs echo | sed 's/ /,/g')
echo ${commaSepratedLibs}

spark-submit --master local  --name consumer --files conf/application.local.conf,conf/application.container.conf --jars ${commaSepratedLibs} --class datapipeline.consumer.KafkaConsumer lib/transcationswithspark.transcationswithspark-0.1.0-SNAPSHOT.jar conf/application.container.conf

#stop consumer once data is loaded into hdfs
spark-submit --master local  --name silverlayer --files conf/application.local.conf,conf/application.container.conf --jars ${commaSepratedLibs} --class datapipeline.cdmlayers.silverlayer.SilverLayerBuilder lib/transcationswithspark.transcationswithspark-0.1.0-SNAPSHOT.jar conf/application.container.conf

spark-submit --master local  --name goldlayer --files conf/application.local.conf,conf/application.container.conf --jars ${commaSepratedLibs} --class datapipeline.cdmlayers.goldlayer.GoldLayerBuilder lib/transcationswithspark.transcationswithspark-0.1.0-SNAPSHOT.jar conf/application.container.conf