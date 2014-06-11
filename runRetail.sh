#hdfs dfs -ls 
#hdfs dfs -mkdir /user/hduser/Retail-in
#hdfs dfs -copyFromLocal /home/hduser/RetailSales/DailySales*.txt /user/hduser/Retail-in
hdfs dfs -ls /user/hduser/Retail-in
hdfs dfs -rm -r /user/hduser/Retail-out
hadoop jar /usr/local/hadoop220/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar -D mapred.job.name='RetailR' -mapper /home/hduser/RetailSales/LinReg-map.R -reducer /home/hduser/RetailSales/LinReg-red.R -input /user/hduser/Retail-in/* -output /user/hduser/Retail-out 
hdfs dfs -ls /user/hduser/Retail-out
