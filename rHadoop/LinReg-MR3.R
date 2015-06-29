#! /usr/bin/env Rscript

Sys.setenv(HADOOP_CMD="/usr/hdp/2.2.0.0-2041/hadoop/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/hdp/2.2.0.0-2041/hadoop-mapreduce/hadoop-streaming-2.6.0.2.2.0.0-2041.jar")
Sys.getenv("HADOOP_CMD")

library(rmr2)
library(rhdfs)
hdfs.init()

EstValue <- function (pSKU,pdays,psale,N){
  days <-as.numeric(scan(text=pdays,,sep=" "))
  sdayO <- days[which.min(days)]
  edayO <- days[which.max(days)]
  days <- days - (sdayO - 1)
  sale <-as.numeric(scan(text=psale,,sep=" "))
  #---------------------------------------
  #PastSale = Reduce("+",sale)
  PastSale = 0
  for (j in 2:length(sale))PastSale = PastSale + sale[j]
  #PastSale= sale[2]
  #----------------------------------------
  regModel <- lm(sale ~ days)              # <-- the all important R function lm()
  regInter = regModel$coefficients[[1]]    # <-- generates intercept
  regSlope = regModel$coefficients[[2]]    # <-- generates slope
  sday <- days[which.min(days)]
  nday <- days[which.max(days)]+1
  lday <- sday+N
  CumSale = 0
  for (day in nday:lday){
    EstSale = regInter + day*regSlope
    CumSale = CumSale + EstSale
  }
  outVal<- paste("dates [",sdayO,"-",edayO,"] :",PastSale," next [",nday,"-",lday,"]",CumSale,":",EstSale," --",(PastSale+CumSale),"-in-",pdays,psale)
  #cat(pSKU,"dates [",sdayO,"-",edayO,"] :",PastSale," next [",nday,"-",lday,"]",CumSale,":",EstSale," --",(PastSale+CumSale),"\n")
  return(outVal)
}

# Linear Regression Mapper Script - written by Prithwis Mukerjee
# Used in the Retail Sales Application

# Reads three pieces of data per line
# date, SKU, sale
# MR Mapper Key is sku
# MR Mapper Value is date$sale 

mapper1 = function(null,line) {
    ckey = line[[2]]
    cval = paste(line[[1]],line[[3]],sep = "$")
    keyval(ckey,cval)
}

# -- the Reducer
# 
# mapOut is the output data from the Mapper script
# read as table : first column = mapkey = SKU name
#               : second column = mapval 
# mapval consists of a string formatted as day$sale
# mapval needs to split into day, sale and then made into list to be passed to EstValue()

reducer1 = function(key,val.list) {
  days <- ""
  sale <- ""
  for(line in val.list) {
    DataVal <- unlist(strsplit(line, split="\\$"))
    days <- paste(days,DataVal[[1]])
    sale <- paste(sale,DataVal[[2]])
  }

  retVal <- EstValue(key,days,sale,9)
  keyval(key,retVal)
}

hdfs.ls("/user/ru1/retail")
hdfs.rm("/user/ru1/retail/out810")

# call MapReduce job
mapreduce(input="/user/ru1/retail/in-csv",
          input.format=make.input.format("csv", sep =","),
          output="/user/ru1/retail/out810",
          output.format="text",
          map=mapper1,
          reduce=reducer1
)

