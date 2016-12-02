library(TTR)
library(rredis)
library(log4r)
library(RTrade)
library(RcppRoll)
library(zoo)
library(plyr)
#library(doParallel)
source("CustomFunctions.R")

writeToRedis = TRUE
datafolder="/home/psharma/Seafile/rfiles/daily/"
getmarketdata = FALSE
strategyname = "contra02"
useSystemDate = FALSE
redisDB=4
dataCutOffBefore="2015-01-01"
BackTestStartDate="2016-04-01"
BackTestEndDate="2017-03-31"
AnnualizedYieldThreshold=0.1

logger <- create.logger()
logfile(logger) <- 'base.log'
level(logger) <- 'INFO'
levellog(logger, "INFO", "Starting BOD Scan")

###### FUNCTIONS ##############
getcontractsize <- function (x, size) {
        a <- size[size$startdate <= as.Date(x) & size$enddate >=as.Date(x),]
        if(nrow(a)>0){
                a<-head(a,1)
        }
        if(nrow(a)>0){
                return(a$contractsize)
        } else
                return(0)
        
}
###### Load Data #############

splits <-
        read.csv(paste(datafolder,"/","splits.csv",sep=""), header = TRUE, stringsAsFactors = FALSE)
symbolchange <-
        read.csv(paste(datafolder,"/","symbolchange.csv",sep=""),
                 header = TRUE,
                 stringsAsFactors = FALSE)
symbolchange <-
        data.frame(key = symbolchange$SM_KEY_SYMBOL,
                   newsymbol = symbolchange$SM_NEW_SYMBOL)
#symbolchange$key = gsub("[^0-9A-Za-z/-]", "", symbolchange$key)
#symbolchange$newsymbol = gsub("[^0-9A-Za-z/-]", "", symbolchange$newsymbol)

splits <-
        data.frame(
                date = as.POSIXct(splits$date, tz = "Asia/Kolkata"),
                symbol = splits$symbol,
                oldshares = splits$oldshares,
                newshares = splits$newshares
        )
#splits$symbol = gsub("[^0-9A-Za-z/-]", "", splits$symbol)

niftysymbols <- createIndexConstituents(2,"nifty50",threshold="2005-01-01")
niftysymbols$symbol<-sapply(niftysymbols$symbol,getMostRecentSymbol,symbolchange$key,symbolchange$newsymbol)
folots<-createFNOSize(2,"contractsize",threshold="2015-01-01")
folots$symbol<-sapply(folots$symbol,getMostRecentSymbol,symbolchange$key,symbolchange$newsymbol)
niftysymbols=niftysymbols[niftysymbols$symbol!="BAJAJHLDNG",]
niftysymbols=niftysymbols[niftysymbols$symbol!="CNXIT",]
niftysymbols=niftysymbols[niftysymbols$symbol!="BANKNIFTY",]
tradingsymbols=niftysymbols$symbol
#tradingsymbols=folots$symbol
tradingsymbols=tradingsymbols[tradingsymbols!="BAJAJHLDNG"]
if(length(grep("CNX",tradingsymbols))>0){
        tradingsymbols=tradingsymbols[-grep("CNX",tradingsymbols)]
}
if(length(grep("NIFTY",tradingsymbols))>0){
        tradingsymbols=tradingsymbols[-grep("NIFTY",tradingsymbols)]
}
tradingsymbols=sapply(tradingsymbols,getMostRecentSymbol,symbolchange$key,symbolchange$newsymbol)
tradingsymbols=unique(tradingsymbols)

invalidsymbols=numeric()
endtime = format(Sys.time(), format = "%Y-%m-%d %H:%M:%S")
#cl <- makeCluster(detectCores())
#registerDoParallel(cl)

if (getmarketdata) {
for (i in 1:length(tradingsymbols)) {
        #         foreach (i =1:nrow(niftysymbols),.packages="RTrade") %dopar% {
                md = data.frame() # create placeholder for market data
                symbol = tradingsymbols[i]
                kairossymbol=symbol
                if (file.exists(paste(datafolder, symbol, ".Rdata", sep = ""))) {
                        load(paste(datafolder, symbol, ".Rdata", sep = ""))
                        if (nrow(md) > 0) {
                                starttime = strftime(md[nrow(md), c("date")] + 1,
                                                     tz = "Asia/Kolkata",
                                                     "%Y-%m-%d %H:%M:%S")
                        }else{
                                starttime = "1995-11-01 09:15:00"
                        }
                } else{
                        starttime = "1995-11-01 09:15:00"
                }
                temp <-
                        kGetOHLCV(
                                paste("symbol", tolower(kairossymbol), sep = "="),
                                df=md,
                                start = starttime,
                                end = endtime,
                                timezone = "Asia/Kolkata",
                                name = "india.nse.equity.s4.daily",
                                ts = c(
                                        "open",
                                        "high",
                                        "low",
                                        "settle",
                                        "close",
                                        "volume",
                                        "delivered"
                                ),
                                aggregators = c(
                                        "first",
                                        "max",
                                        "min",
                                        "last",
                                        "last",
                                        "sum",
                                        "sum"
                                ),
                                aValue = "1",
                                aUnit = "days",
                                splits = splits,
                                symbolchange = symbolchange
                                
                        )
                #md <- rbind(md, temp)
                md<-temp
                if(nrow(md)>0){
                        md$symbol<-symbol
                        save(md,
                             file = paste(datafolder, symbol, ".Rdata", sep = "")) # save new market data to disc
                }
                
        }
        #stopCluster(cl)
        
        # get index data
        md = data.frame() # create placeholder for market data
        symbol = "NSENIFTY"
        #kairossymbol=gsub("[^0-9A-Za-z///' ]","",symbol)
        kairossymbol=symbol
        if (file.exists(paste(datafolder, symbol, ".Rdata", sep = ""))) {
                load(paste(datafolder, symbol, ".Rdata", sep = ""))
                if (nrow(md) > 0) {
                        starttime = strftime(md[nrow(md), c("date")] + 1,
                                             tz = "Asia/Kolkata",
                                             "%Y-%m-%d %H:%M:%S")
                }else{
                        starttime = "1995-11-01 09:15:00"
                }
        } else{
                starttime = "1995-11-01 09:15:00"
        }
        temp <-
                kGetOHLCV(
                        paste("symbol", tolower(kairossymbol), sep = "="),
                        df=md,
                        start = starttime,
                        end = endtime,
                        timezone = "Asia/Kolkata",
                        name = "india.nse.index.s4.daily",
                        ts = c(
                                "open",
                                "high",
                                "low",
                                "settle",
                                "close",
                                "volume"
                        ),
                        aggregators = c(
                                "first",
                                "max",
                                "min",
                                "last",
                                "last",
                                "sum"
                        ),
                        aValue = "1",
                        aUnit = "days",
                        splits = splits,
                        symbolchange = symbolchange
                        
                )
        #md <- rbind(md, temp)
        md<-temp
        md$symbol<-symbol
        save(md,
             file = paste(datafolder, symbol, ".Rdata", sep = "")) # save new market data to disc
        index<-md
        
        
}
load(paste(datafolder, "NSENIFTY", ".Rdata", sep = ""))
NSENIFTY<-md

##### 1. Calculate Indicators ########
MALongPeriod = 200
EntryRSI = 80
ExitRSI = 20
RSIPeriod = 2
ExitRSIPeriod = 2
HighRSIBars = 2

slope <- function (x) {
        res <- (lm(log(x) ~ seq(1:length(x))))
        res$coefficients[2]
}

r2 <- function(x) {
        res <- (lm(log(x) ~ seq(1:length(x))))
        summary(res)$r.squared
}


signals <- data.frame()
allmd <- list()
for (i in 1:length(tradingsymbols)) {
        #for (i in 1:10) {
        print(i)
        symbol = tradingsymbols[i]
        if(file.exists(paste(datafolder, symbol, ".Rdata", sep = ""))){
        load(paste(datafolder, symbol, ".Rdata", sep = "")) #loads md
        md <- md[md$date >= dataCutOffBefore,]
        if(nrow(md)>MALongPeriod){
        md$CurrentRSI = RSI(md$asettle, RSIPeriod)
       # md$IndexCloseBelowLongTermMA = NSENIFTY$asettle < EMA(NSENIFTY$asettle, MALongPeriod)
        index=which(niftysymbols$symbol==symbol)
        if(length(index)>0){
                #eligible if nifty index did not contain symbol on the date
                md$eligible=1
                for(j in 1:length(index)){
                        md$eligible=md$eligible*ifelse(as.Date(md$date)>=niftysymbols[index[j],c("startdate")] & as.Date(md$date)<=niftysymbols[index[j],c("enddate")],0,1)
        #                md$eligible=md$eligible & md$IndexCloseBelowLongTermMA
                }
                #md$eligible=ifelse(as.Date(md$date)>=niftysymbols[i,c("startdate")] & as.Date(md$date)>niftysymbols[i,c("enddate")],1,0)
        }else{
                md$eligible=1      
         #       md$eligible=md$eligible & md$IndexCloseBelowLongTermMA
        }
        #md$eligible=ifelse(length(which(niftysymbols$symbol==symbol))>0,0,1)
        md$CloseBelowLongTermMA = md$asettle < EMA(md$asettle, MALongPeriod)
        md$HigherClose = md$asettle > Ref(md$asettle, -1)
        md$MultipleDayHighRSI = runSum(RSI(md$asettle, RSIPeriod) > EntryRSI, HighRSIBars) ==
                HighRSIBars
        AnnualizedSlope = (exp(rollapply(md$asettle, 90, slope)) ^ 252) - 1
        md$AnnualizedSlope <-
                c(rep(NA, nrow(md) - length(AnnualizedSlope)), AnnualizedSlope)
        r <- rollapply(md$asettle, 90, r2)
        md$r <- c(rep(NA, nrow(md) - length(r)), r)

        ####### 2. Generate Buy/Sell Arrays ##########
        md$cover = Cross(RSI(md$asettle, ExitRSIPeriod), ExitRSI)

        BarsSinceCover = BarsSince(md$cover)

        md$firstentry = md$CloseBelowLongTermMA &
                md$MultipleDayHighRSI &
                md$AnnualizedSlope < AnnualizedYieldThreshold & md$r > 0.7
        InFirstPos = Flip(md$firstentry, md$cover)

        FirstTrigger = ExRem(InFirstPos, md$cover)

        BarsSinceFirstTrigger = BarsSince(FirstTrigger)
        FirstTriggerPrice = ifelse(
                BarsSinceFirstTrigger < BarsSinceCover,
                Ref(md$asettle,-BarsSinceFirstTrigger),
                0
        )


        SecondEntry = md$CloseBelowLongTermMA &
                md$asettle > FirstTriggerPrice &
                InFirstPos &
                Ref(InFirstPos,-1) & md$AnnualizedSlope < AnnualizedYieldThreshold

        InSecondPos = Flip(SecondEntry, md$cover)

        SecondTrigger = ExRem(InSecondPos, md$cover)

        BarsSinceSecondTrigger = BarsSince(SecondTrigger)

        SecondTriggerPrice = ifelse(
                BarsSinceSecondTrigger < BarsSinceCover,
                Ref(md$asettle,-BarsSinceSecondTrigger),
                0
        )


        ThirdEntry = md$CloseBelowLongTermMA &
                md$asettle > SecondTriggerPrice &
                InSecondPos &
                Ref(InSecondPos,-1) & md$AnnualizedSlope < AnnualizedYieldThreshold

        InThirdPos = Flip(ThirdEntry, md$cover)

        ThirdTrigger = ExRem(InThirdPos, md$cover)

        BarsSinceThirdTrigger = BarsSince(ThirdTrigger)

        ThirdTriggerPrice = ifelse(
                BarsSinceThirdTrigger < BarsSinceCover,
                Ref(md$asettle,-BarsSinceThirdTrigger),
                0
        )


        FourthEntry = md$CloseBelowLongTermMA &
                md$asettle > ThirdTriggerPrice & InThirdPos &
                Ref(InThirdPos,-1) & md$AnnualizedSlope < AnnualizedYieldThreshold

        InFourthPos = Flip(FourthEntry, md$cover)

        FourthTrigger = ExRem(InFourthPos, md$cover)

        BarsSinceFourthTrigger = BarsSince(FourthTrigger)

        FourthTriggerPrice = ifelse(
                BarsSinceFourthTrigger < BarsSinceCover,
                Ref(md$asettle,-BarsSinceFourthTrigger),
                0
        )

        md$positionscore = 100 + md$CurrentRSI + (EMA(md$asettle, 20)-md$asettle) *
                100 / md$asettle


        ####### 2. Generate Buy/Sell Arrays ##########
        md$short = ifelse(FirstTrigger,
                        1,
                        ifelse(SecondTrigger |
                                       ThirdTrigger |
                                       FourthTrigger, 999, 0))
        md$short = ifelse(md$eligible==1,md$short,0)
        allsize = folots[folots$symbol == symbol,]
        size = sapply(md$date, getcontractsize, allsize)
        md$short=ifelse(size>0,md$short,0)
        
        md$cover = ExRem(md$cover, md$short)

        md$buy = 0

        md$sell = 0

        md$shortprice = md$settle

        md$coverprice = md$settle

        md$buyprice = md$settle

        md$sellprice = md$settle
        md <- md[md$date >= BackTestStartDate & md$date <= BackTestEndDate,]
        allmd[[i]]<-md
        signals <-
                rbind(md[, c(
                        "date",
                        "aopen",
                        "ahigh",
                        "alow",
                        "asettle",
                        "buy",
                        "sell",
                        "short",
                        "cover",
                        "buyprice",
                        "shortprice",
                        "sellprice",
                        "coverprice",
                        "positionscore",
                        "symbol"
                )], signals)
        }else{
                invalidsymbols<-c(invalidsymbols,i)
        }
}
        }

signals <- na.omit(signals)
signals$aclose <- signals$asettle
dates <- unique(signals[order(signals$date), c("date")])
a <- ProcessPositionScoreShort(signals, 5, dates)
# Generate consolidated buy/sell
processedsignals <- ApplyStop(a, rep(10000000, nrow(a)))
processedsignals <- processedsignals[order(processedsignals$date),]
trades <- GenerateTrades(processedsignals)
trades <- trades[order(trades$entrytime),]
trades$brokerage <-
        (trades$entryprice * 0.0002 + trades$exitprice * 0.0002) / trades$entryprice
trades$netpercentprofit <-
        trades$percentprofit - trades$brokerage


# for (i in 1:nrow(niftysymbols)) {
#         symbol = niftysymbols$Symbol[i]
#         load(paste("../daily/", symbol, ".Rdata", sep = "")) #loads md
#         allmd[[i]] = md[md$date > "2015-01-01",]
# }



allequity <- list()

for (i in 1:length(tradingsymbols)) {
        symbol = tradingsymbols[i]
        md <- allmd[[i]]
        if(!is.null(md) && nrow(md)>0){
                allsize = folots[folots$symbol == symbol,]
                size = sapply(md$date, getcontractsize, allsize)
                datesformd <- md[md$symbol == symbol, c("date")]
                allequity[[i]] = CalculatePortfolioEquityCurve(symbol, md, trades, size, 0.0002)
        
        }
}

pp <- rbind.fill(allequity)
sumequity <- ddply(pp, .(date), function(x)
        colSums(x[,-1], na.rm = TRUE))


########### SAVE SIGNALS TO REDIS #################
if (useSystemDate) {
        today = Sys.Date()
        today = as.POSIXct(format(today), tz = "Asia/Kolkata")
} else{
        today = sumequity[nrow(sumequity), c("date")]
}

if (writeToRedis) {
        levellog(logger, "INFO", "Saving trades to Redis")
        redisConnect()
        redisSelect(as.numeric(redisDB))
        vcover = processedsignals[processedsignals$date==today, c("symbol", "cover")]
        vcover = vcover[vcover$cover == 1,]
        if (nrow(vcover) > 0) {
                for (i in 1:nrow(vcover)) {
                        strategyside = ifelse(nrow(vcover) >= 1, "COVER", "AVOID")
                        #index = which(niftysymbols$symbol == vcover$symbol[i])
                        index = which(tradingsymbols == vcover$symbol[i])
                        equity <- allequity[[index]]
                        equityarrayindex = which(equity$date == today)
                        contractstoday = equity[equityarrayindex, c("contracts")][1]
                        contractsyday = equity[equityarrayindex - 1, c("contracts")][1]
                        positionsize=contractsyday
                        exitsize = contractsyday - contractstoday
                        if (exitsize < 0) {
                                symbolname = paste(vcover$symbol[i],
                                                   "_STK___",
                                                   sep = "")
                                redisRPush(
                                        paste("trades", strategyname , sep = ":"),
                                        charToRaw(
                                                paste(
                                                        symbolname,
                                                        abs(exitsize),
                                                        strategyside,
                                                        0,
							abs(positionsize),
                                                        sep = ":"
                                                )
                                        )
                                )
                                levellog(
                                        logger,
                                        "INFO",
                                        paste(
                                                strategyname,
                                                abs(exitsize),
                                                strategyside,
                                                0,
                                                positionsize,
                                                sep = ":"
                                        )
                                )
                        }
                }
        }


        #save entry action to redis
        vshort = processedsignals[processedsignals$date==today, c("symbol", "short")]
        vshort = vshort[vshort$short > 0,]
        if (nrow(vshort) > 0) {
                for (i in 1:nrow(vshort)) {
                        strategyside = ifelse(nrow(vshort) >= 1, "SHORT", "AVOID")
                        #index = which(niftysymbols$symbol == vshort$symbol[i])
                        index = which(tradingsymbols == vshort$symbol[i])
                        equity <- allequity[[index]]
                        equityarrayindex = which(equity$date == today)
                        contractstoday = equity[equityarrayindex, c("contracts")][1]
                        contractsyday = equity[equityarrayindex - 1, c("contracts")][1]

                        entrysize = contractstoday - contractsyday
                        positionsize=contractsyday
                        if (entrysize < 0) {
                                symbolname = paste(vshort$symbol[i],
                                                   "_STK___",
                                                   sep = "")
                                redisRPush(
                                        paste("trades", strategyname , sep = ":"),
                                        charToRaw(
                                                paste(
                                                        symbolname,
                                                        abs(entrysize),
                                                        strategyside,
                                                        0,
                                                        abs(positionsize),
                                                        sep = ":"
                                                )
                                        )
                                )
                                levellog(
                                        logger,
                                        "INFO",
                                        paste(
                                                strategyname,
                                                abs(entrysize),
                                                strategyside,
                                                0,
                                                sep = ":"
                                        )
                                )
                        }
                }
        }


        redisClose()
}
winratio=nrow(trades[trades$netpercentprofit>0,])/nrow(trades)
getposition<-function(allequity,index){
        out=numeric()
        for(i in 1:length(index)){
                position<-tail(allequity[[index[i]]],1)$contracts
                out=c(out,position)
        }
        out
}
gettradecount<-function(trades){
        out=numeric()
        psymbol=unique(trades[trades$exitprice==0,c("symbol")])
        for(i in 1:length(psymbol)){
                tradessubset<-trades[trades$exitprice==0 & trades$symbol==psymbol[i],]
                out=c(out,nrow(tradessubset))
        }
        out
}

print(trades[trades$exitprice==0 | as.Date(trades$exittime,tz="Asia/Kolkata")==as.Date(today,tz="Asia/Kolkata"),])
psymbol=unique(trades[trades$exitprice==0,c("symbol")])
#pindex=match(psymbol,niftysymbols$symbol)
pindex=match(psymbol,tradingsymbols)
if(length(pindex)>0){
        summary<-data.frame(symbol=psymbol,position=getposition(allequity,pindex),tradecount=gettradecount(trades))
        print(summary)
        
}

