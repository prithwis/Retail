#! /usr/bin/env Rscript

# Linear Regression - written by Prithwis Mukerjee
# Used in the Retail Sales Application


# This version MR4 is a cleaned up version of MR3
# that makes it look more professional !

Sys.setenv(HADOOP_CMD="/usr/hdp/2.2.0.0-2041/hadoop/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/hdp/2.2.0.0-2041/hadoop-mapreduce/hadoop-streaming-2.6.0.2.2.0.0-2041.jar")
Sys.getenv("HADOOP_CMD")

library(rmr2)
library(rhdfs)
hdfs.init()

LinReg <- function (pSKU,pdays,psale,N){
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
  RetVal<- paste("dates [",sdayO,"-",edayO,"] :",PastSale," next [",nday,"-",lday,"]",CumSale,":",EstSale," --",(PastSale+CumSale),"-in-",pdays,psale)
  return(RetVal)
}


# Reads three pieces of data per line
# date, SKU, sale
# MR Mapper Key is sku
# MR Mapper Value is date$sale 

map = function(null,line) {
    key = line[[2]]
    val = paste(line[[1]],line[[3]],sep = "$")
    keyval(key,val)
}


# mapval consists of a string formatted as day$sale
# mapval needs to split into day, sale and then made into list to be passed to EstValue()

reduce = function(key,val.list) {
  days <- ""
  sale <- ""
  for(line in val.list) {
    DataVal <- unlist(strsplit(line, split="\\$"))
    days <- paste(days,DataVal[[1]])
    sale <- paste(sale,DataVal[[2]])
  }

  RegOutPut <- LinReg(key,days,sale,9)
  keyval(key,RegOutPut)
}



# call MapReduce job
RetailSales = function(input, output = NULL) {
  mapreduce(input=input,
            output = output, 
            input.format=make.input.format("csv", sep =","),
            output.format="text",
            map=map,
            reduce=reduce
            )
}

## Submit job

#hdfs.rm("/user/ru1/retail/out810")

hdfs.root <- '/user/ru1/retail'
hdfs.ls(hdfs.root)
hdfs.data <- file.path(hdfs.root, 'in-csv') 
hdfs.out <- file.path(hdfs.root, 'out510') 

hdfs.rm(hdfs.out)
out <- RetailSales(hdfs.data, hdfs.out)

## Fetch results from HDFS
results <- from.dfs(hdfs.out,format="text")
results
