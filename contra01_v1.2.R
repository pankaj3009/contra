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
getmarketdata =FALSE
strategyname = "contra01"
useSystemDate = FALSE
redisDB=4
dataCutOffBefore="2015-01-01"
BackTestStartDate="2016-04-01"
BackTestEndDate="2017-03-31"


logger <- create.logger()
logfile(logger) <- 'base.log'
level(logger) <- 'INFO'
levellog(logger, "INFO", "Starting BOD Scan")

###### BACKTEST ##############

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
symbolchange$key = gsub("[^0-9A-Za-z/-]", "", symbolchange$key)
symbolchange$newsymbol = gsub("[^0-9A-Za-z/-]", "", symbolchange$newsymbol)

splits <-
        data.frame(
                date = as.POSIXct(splits$date, tz = "Asia/Kolkata"),
                symbol = splits$symbol,
                oldshares = splits$oldshares,
                newshares = splits$newshares
        )
splits$symbol = gsub("[^0-9A-Za-z/-]", "", splits$symbol)

niftysymbols <- createIndexConstituents(2,"nifty50",threshold="2015-01-01")
folots<-createFNOSize(2,"contractsize",threshold="2015-01-01")

invalidsymbols=numeric()
endtime = format(Sys.time(), format = "%Y-%m-%d %H:%M:%S")
#cl <- makeCluster(detectCores())
#registerDoParallel(cl)
if (getmarketdata) {
       for (i in 1:nrow(niftysymbols)) {
       # foreach (i =1:nrow(niftysymbols),.packages="RTrade") %dopar% {
                md = data.frame() # create placeholder for market data
                symbol = niftysymbols$symbol[i]
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
                md$symbol<-symbol
                save(md,
                     file = paste(datafolder, symbol, ".Rdata", sep = "")) # save new market data to disc

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




##### 1. Calculate Indicators ########
MALongPeriod = 200
EntryRSI = 20
ExitRSI = 80
RSIPeriod = 2
ExitRSIPeriod = 2
LowRSIBars = 2

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
for (i in 1:nrow(niftysymbols)) {
        #for (i in 1:10) {
        print(i)
        symbol = niftysymbols$symbol[i]
        load(paste(datafolder, symbol, ".Rdata", sep = "")) #loads md
        md <- md[md$date >= dataCutOffBefore,]
        if(nrow(md)>MALongPeriod){
        md$CurrentRSI = RSI(md$asettle, RSIPeriod)
        md$eligible=ifelse(as.Date(md$date)>=niftysymbols[i,c("startdate")] & as.Date(md$date)<=niftysymbols[i,c("enddate")],1,0)
        md$CurrentLTRSI=RSI(md$asettle,14)
        md$CloseAboveLongTermMA = md$asettle > EMA(md$asettle, MALongPeriod)
        md$LowerClose = md$asettle < Ref(md$asettle, -1)
        md$MultipleDayLowRSI = runSum(RSI(md$asettle, 2) < EntryRSI, LowRSIBars) ==
                LowRSIBars
        AnnualizedSlope = (exp(rollapply(md$asettle, 90, slope)) ^ 252) - 1
        md$AnnualizedSlope <-
                c(rep(NA, nrow(md) - length(AnnualizedSlope)), AnnualizedSlope)
        r <- rollapply(md$asettle, 90, r2)
        md$r <- c(rep(NA, nrow(md) - length(r)), r)

        ####### 2. Generate Buy/Sell Arrays ##########
        md$sell = Cross(RSI(md$asettle, ExitRSIPeriod), ExitRSI)

        BarsSinceSell = BarsSince(md$sell)

        md$firstentry = md$CloseAboveLongTermMA &
                md$MultipleDayLowRSI &
                md$AnnualizedSlope > 0 & md$r > 0.7 &
                md$CurrentLTRSI >0.7
        InFirstPos = Flip(md$firstentry, md$sell)

        FirstTrigger = ExRem(InFirstPos, md$sell)

        BarsSinceFirstTrigger = BarsSince(FirstTrigger)
        FirstTriggerPrice = ifelse(
                BarsSinceFirstTrigger < BarsSinceSell,
                Ref(md$asettle,-BarsSinceFirstTrigger),
                0
        )


        SecondEntry = md$CloseAboveLongTermMA &
                md$asettle < FirstTriggerPrice &
                InFirstPos &
                Ref(InFirstPos,-1) & md$AnnualizedSlope > 0 & 
                md$CurrentLTRSI >0.7

        InSecondPos = Flip(SecondEntry, md$sell)

        SecondTrigger = ExRem(InSecondPos, md$sell)

        BarsSinceSecondTrigger = BarsSince(SecondTrigger)

        SecondTriggerPrice = ifelse(
                BarsSinceSecondTrigger < BarsSinceSell,
                Ref(md$asettle,-BarsSinceSecondTrigger),
                0
        )


        ThirdEntry = md$CloseAboveLongTermMA &
                md$asettle < SecondTriggerPrice &
                InSecondPos &
                Ref(InSecondPos,-1) & md$AnnualizedSlope > 0 &
                md$CurrentLTRSI >0.7

        InThirdPos = Flip(ThirdEntry, md$sell)

        ThirdTrigger = ExRem(InThirdPos, md$sell)

        BarsSinceThirdTrigger = BarsSince(ThirdTrigger)

        ThirdTriggerPrice = ifelse(
                BarsSinceThirdTrigger < BarsSinceSell,
                Ref(md$asettle,-BarsSinceThirdTrigger),
                0
        )


        FourthEntry = md$CloseAboveLongTermMA &
                md$asettle < ThirdTriggerPrice & InThirdPos &
                Ref(InThirdPos,-1) & md$AnnualizedSlope > 0 &
                md$CurrentLTRSI >0.7

        InFourthPos = Flip(FourthEntry, md$sell)

        FourthTrigger = ExRem(InFourthPos, md$sell)

        BarsSinceFourthTrigger = BarsSince(FourthTrigger)

        FourthTriggerPrice = ifelse(
                BarsSinceFourthTrigger < BarsSinceSell,
                Ref(md$asettle,-BarsSinceFourthTrigger),
                0
        )

        md$positionscore = 100 - md$CurrentRSI + (md$asettle - EMA(md$asettle, 20)) *
                100 / md$asettle


        ####### 2. Generate Buy/Sell Arrays ##########
        md$buy = ifelse(FirstTrigger,
                        1,
                        ifelse(SecondTrigger |
                                       ThirdTrigger |
                                       FourthTrigger, 999, 0))
        md$buy = ifelse(md$eligible==1,md$buy,0)
        md$sell = ExRem(md$sell, md$buy)

        md$short = 0

        md$cover = 0

        md$buyprice = md$settle

        md$sellprice = md$settle

        md$shortprice = md$settle

        md$coverprice = md$settle
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
#niftysymbols<-niftysymbols[-invalidsymbols,]

signals <- na.omit(signals)
signals$aclose <- signals$asettle
dates <- unique(signals[order(signals$date), c("date")])
a <- ProcessPositionScore(signals, 5, dates)
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
#         load(paste(datafolder, symbol, ".Rdata", sep = "")) #loads md
#         allmd[[i]] = md[md$date > "2015-01-01",]
# }

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

allequity <- list()

for (i in 1:nrow(niftysymbols)) {
        if(!i %in% invalidsymbols){
        symbol = niftysymbols$symbol[i]
        print(paste(i,symbol,sep=":"))
        md <- allmd[[i]]
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
        vsell = processedsignals[(nrow(processedsignals) - 50):nrow(processedsignals), c("symbol", "sell")]
        vsell = vsell[vsell$sell == 1,]
        if (nrow(vsell) > 0) {
                for (i in 1:nrow(vsell)) {
                        strategyside = ifelse(nrow(vsell) >= 1, "SELL", "AVOID")
                        index = which(niftysymbols$symbol == vsell$symbol[i])
                        equity <- allequity[[index]]
                        equityarrayindex = which(equity$date == today)
                        contractstoday = equity[equityarrayindex, c("contracts")][1]
                        contractsyday = equity[equityarrayindex - 1, c("contracts")][1]
                        positionsize=contractsyday
                        exitsize = contractsyday - contractstoday
                        if (exitsize > 0) {
                                symbolname = paste(vsell$symbol[i],
                                                   "_STK___",
                                                   sep = "")
                                redisRPush(
                                        paste("trades", strategyname , sep = ":"),
                                        charToRaw(
                                                paste(
                                                        symbolname,
                                                        exitsize,
                                                        strategyside,
                                                        0,
							positionsize,
                                                        sep = ":"
                                                )
                                        )
                                )
                                levellog(
                                        logger,
                                        "INFO",
                                        paste(
                                                strategyname,
                                                exitsize,
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
        vbuy = processedsignals[(nrow(processedsignals) - 50):nrow(processedsignals), c("symbol", "buy")]
        vbuy = vbuy[vbuy$buy > 0,]
        if (nrow(vbuy) > 0) {
                for (i in 1:nrow(vbuy)) {
                        strategyside = ifelse(nrow(vbuy) >= 1, "BUY", "AVOID")
                        index = which(niftysymbols$symbol == vbuy$symbol[i])
                        equity <- allequity[[index]]
                        equityarrayindex = which(equity$date == today)
                        contractstoday = equity[equityarrayindex, c("contracts")][1]
                        contractsyday = equity[equityarrayindex - 1, c("contracts")][1]

                        entrysize = contractstoday - contractsyday
                        positionsize=contractsyday
                        if (entrysize > 0) {
                                symbolname = paste(vbuy$symbol[i],
                                                   "_STK___",
                                                   sep = "")
                                redisRPush(
                                        paste("trades", strategyname , sep = ":"),
                                        charToRaw(
                                                paste(
                                                        symbolname,
                                                        entrysize,
                                                        strategyside,
                                                        0,
                                                        positionsize,
                                                        sep = ":"
                                                )
                                        )
                                )
                                levellog(
                                        logger,
                                        "INFO",
                                        paste(
                                                strategyname,
                                                entrysize,
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
pindex=match(psymbol,niftysymbols$symbol)
summary<-data.frame(symbol=psymbol,position=getposition(allequity,pindex),tradecount=gettradecount(trades))
print(summary)
#tail(allmd[[which(niftysymbols$Symbol=="BPCL")]])
