# -- used in the Hortonworks HDP .. two -file commands required
hdfs dfs -rm -r /user/ru1/retail/out900
hadoop jar /usr/hdp/2.2.0.0-2041/hadoop-mapreduce/hadoop-streaming-2.6.0.2.2.0.0-2041.jar -D mapred.job.name='RetailR' -mapper /home/ru1/retail/LinReg-map.R -reducer /home/ru1/retail/LinReg-red.R -input /user/ru1/retail/in-txt -output /user/ru1/retail/out900 -file LinReg-map.R -file LinReg-red.R 

