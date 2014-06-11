#! /usr/bin/env Rscript

# Linear Regression Mapper Script - written by Prithwis Mukerjee
# Used in the Retail Sales Application

# Reads three pieces of data per line
# date, SKU, sale
# MR Mapper Key is sku
# MR Mapper Value is date$sale 

DailySales <- read.table("stdin",col.names=c("date","sku","sale"))
for(i in 1:nrow(DailySales)){
  Key <- as.character(DailySales[i,]$sku)
  Val <- paste(as.character(DailySales[i,]$date),"$",as.character(DailySales[i,]$sale))
  cat(Key,gsub(" ","",Val),"\n")
}

