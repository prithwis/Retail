#! /usr/bin/env Rscript

# Linear Regression Reducer Script - written by Prithwis Mukerjee
# Used in the Retail Sales Application

# EstValue is function called to implement the linear regression function of R
#
# Parameters are as follows
# pSKU - string - name of SKU
# pdays - list of days for which data is available
# psale - list of values of sale data
# N - integer - number of days after start date till which estimate will be made
#
# Other variables are as follows
# sday0 - first day for which sales data is available
# eday0 - last day for which sales data is available
# PastSale - cumulative sales data for days for which data is available
# sday - first day for which data is available, after day data is normalised to start from 1
# nday - first day for which data is not available, after day data is normalised to start from 1
# lday - last day for which data is to be estimated, after day data is normalised to start from 1
# CumSale - estimated cumulative sale from nday to lday
# EstSale - estimated sale for a particular day
#
# regModel - linear regression model created by R function lm()
# regInter - value of intercept of regresion model
# regSlope - value of slope of regression model

EstValue <- function (pSKU,pdays,psale,N){
  days <-as.numeric(scan(text=pdays,,sep=" "))
  sdayO <- days[which.min(days)]
  edayO <- days[which.max(days)]
  days <- days - (sdayO - 1)
  sale <-as.numeric(scan(text=psale,,sep=" "))
  PastSale = Reduce("+",sale)
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
  cat(pSKU,"dates [",sdayO,"-",edayO,"] :",PastSale," next [",nday,"-",lday,"]",CumSale,":",EstSale," --",(PastSale+CumSale),"\n")
  
}

# -- the Reducer
# 
# mapOut is the output data from the Mapper script
# read as table : first column = mapkey = SKU name
#               : second column = mapval 
# mapval consists of a string formatted as day$sale
# mapval needs to split into day, sale and then made into list to be passed to EstValue()

mapOut <- read.table("stdin",col.names=c("mapkey","mapval"))
CurrSKU <- as.character(mapOut[1,]$mapkey)
CurrVal <- ""
days <- ""
sale <- ""
FIRSTROW = TRUE
for(i in 1:nrow(mapOut)){
  SKU <- as.character(mapOut[i,]$mapkey)
  Val <- as.character(mapOut[i,]$mapval)
  DataVal <- unlist(strsplit(Val,"\\$"))
  if (identical(SKU,CurrSKU)){
    CurrVal = paste(CurrVal, Val)
    if (FIRSTROW)  {
      days <- DataVal[1]
      sale <- DataVal[2]
      FIRSTROW = FALSE
    } else {
    days = paste(days,DataVal[1])
    sale = paste(sale,DataVal[2])
  }
  }
  else {
    EstValue(CurrSKU,days,sale,9)
    CurrSKU <- SKU
    CurrVal <- Val
    days <- DataVal[1]
    sale <- DataVal[2]
    
  }
}
EstValue(CurrSKU,days,sale,9)
