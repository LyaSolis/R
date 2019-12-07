########################################################################################################################

###################     DIM_DATE TABLE IS CREATED AND POPULATED DIRECTLY IN SQL SERVER    ##############################

########################################################################################################################
#***********************************************************************************************************************
########################################################################################################################

################################             SET UP ENVIRONMENT        #################################################

########################################################################################################################

#install.packages('curl')
library(curl)
#install.packages('devtools')
library('devtools')
#devtools::install_github("jessevent/crypto")
library(crypto)
library(jsonlite)
#install.packages('plyr')
library(plyr)
library(dplyr)
library(doSNOW)
#install.packages('doParallel')
library(doParallel)
library(lubridate)
library(crypto)
library(curl)
#install.packages("RODBC")
library(RODBC)
#install.packages('sqldf')
library(sqldf)
########################################################################################################################

############################    GET HISTORICAL CRYPTO RATES FOR 2018   #################################################

########################################################################################################################

all_coins <- crypto_history(start_date = '20180101')
recent <- crypto_history(start_date = '20180722')
total <- rbind(all_coins, recent)
total <- unique(total)
#yest <- crypto_history(start_date = '2018-07-29')
head(total)
names(total)[names(total) == 'slug'] <- 'CoinID'
names(total)[names(total) == "date"] <- 'Date'
names(total)[names(total) == "ranknow"] <- 'Rank'
names(total)[names(total) == "market_cap_usd"] <- "Market"


#saveRDS(all_coins, 'D:/DataWarehousing/usable/crypto_history_2018.rds')
# retrieving market history for specific crypto currency
# coin <- "kin"
# kin_coins <- listCoins(coin)

# retrieving market history for ALL crypto currencies
#all_exchanges <- crypto_exchanges()

write.csv(total, "D:/Datawarehousing/total.csv", row.names = FALSE, quote = FALSE)

# /*------------------------
#   use Currency_DB
# CREATE TABLE Hist_FACT (
#   CoinID varchar(255),
#   symbol varchar(255),
#   [name] varchar(255),
#   [Date] varchar(50),
#   [rank] int, 
#   [open] float,
#   high  float,
#   low  float,
#   [close] float,
#   volume float,
#   market float,
#   close_ratio float,
#   spread float 
# );   
# ------------------------*/
  
cn <- odbcConnect('Currency_DB')
sqlQuery(cn, paste0("BULK INSERT Hist_FACT FROM 'D:/Datawarehousing/total.csv' 
                    WITH (FIRSTROW =2, FIELDTERMINATOR = ',',
                    ROWTERMINATOR = '\n')"))


########################################################################################################################

###########################     DIM_RANK Table - Populate in SQL Server             ####################################

########################################################################################################################

rank <- as.data.frame(c(1:3000))

#Load into SQL DB:
write.csv(rank, "D:/Datawarehousing/Rank.csv", row.names = FALSE, quote = FALSE)

cn <- odbcConnect('Currency_DB')
sqlQuery(cn, paste0("BULK INSERT RankDimention FROM 'D:/Datawarehousing/Rank.csv' 
                    WITH (FIRSTROW =2, FIELDTERMINATOR = ',',
                    ROWTERMINATOR = '\n')"))

########################################################################################################################

###########################     FACT_CRYPTO DAILY PRICES OF CRYPTO CURRENCIES       ####################################

########################################################################################################################

# This will retrieve the current market prices from CoinMarketCap. Data gets refreshed every 5 minutes.
prices <- crypto_prices(coin = NULL, limit = 0, currency = NULL)

names(prices)[names(prices) == 'id'] <- 'CoinID'
names(prices)[names(prices) == "last_updated"] <- 'Date'
prices$Date <- substr(prices$Date, 1,10)

prices <- prices[, c("CoinID", "name", "symbol", "rank", "price_usd", "market_cap_usd", "available_supply", "total_supply", "max_supply", "percent_change_24h", "percent_change_7d", 'Date')]
head(prices)
# Create a table in SQL server
# /*------------------------
# use Currency_DB
# # CREATE TABLE Temp_FACT (
#   CoinID varchar(255),
#   [name] varchar(255),
#   symbol varchar(255),
#   [rank] int, 
#   price_usd float, 
#   market_cap_usd float, 
#   available_supply float,
#   total_supply float,
#   max_supply float,
#   percent_change_24h float,
#   percent_change_7d float,
#   [Date] varchar(50)
# );
# ------------------------*/
  
#Load into SQL DB:
write.csv(prices, "D:/Datawarehousing/prices.csv", row.names = FALSE, quote = FALSE)


cn <- odbcConnect('Currency_DB')
sqlQuery(cn, paste0("BULK INSERT Temp_FACT FROM 'D:/Datawarehousing/prices.csv' 
                    WITH (FIRSTROW =2, FIELDTERMINATOR = ',',
                    ROWTERMINATOR = '\n')"))


########################################################################################################################

###########################                 DIM_COIN TABLE POPULATE                 ####################################

########################################################################################################################

dim_coin <- prices[, c("id","name","symbol")]
dim_coin <- unique(dim_coin)
write.csv(dim_coin, "D:/Datawarehousing/dim_coin.csv", row.names = FALSE, quote = FALSE)
#create a table in SQL Server:
# use Currency_DB
# CREATE TABLE CoinDimention (
#   ID varchar(255),
#   [name] varchar(255),
#   symbol varchar(255)
# );
cn <- odbcConnect('Currency_DB')
sqlQuery(cn, paste0("BULK INSERT CoinDimention FROM 'D:/Datawarehousing/dim_coin.csv' 
                    WITH (FIRSTROW =2, FIELDTERMINATOR = ',',
                    ROWTERMINATOR = '\n')"))

