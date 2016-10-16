library(TTR)
library(rredis)
library(log4r)
library(RTrade)
library(RcppRoll)
library(zoo)
library(plyr)

writeToRedis = TRUE

#Uncomment the code below for testing
writeToRedis = TRUE
getmarketdata = TRUE
strategyname = "contra01"
useSystemDate = FALSE
redisDB=4


logger <- create.logger()
logfile(logger) <- 'base.log'
level(logger) <- 'INFO'
levellog(logger, "INFO", "Starting BOD Scan")

###### BACKTEST ##############

###### Load Data #############

splits <-
        read.csv("splits.csv", header = TRUE, stringsAsFactors = FALSE)
symbolchange <-
        read.csv("symbolchange.csv",
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

niftysymbols <-
        read.csv("ind_nifty50list.csv",
                 header = TRUE,
                 stringsAsFactors = FALSE)
folots <-
        read.csv("fo_mktlots.csv",
                 header = TRUE,
                 stringsAsFactors = FALSE)
folots$Symbol <- gsub("[^0-9A-Za-z/-]", "", folots$Symbol)
folots$EffectiveDate = as.POSIXct(folots$EffectiveDate, tz = "Asia/Kolkata")

niftysymbols$Symbol <-
        gsub("[^0-9A-Za-z/-]", "", niftysymbols$Symbol)
endtime = format(Sys.time(), format = "%Y-%m-%d %H:%M:%S")
if (getmarketdata) {
        for (i in 1:nrow(niftysymbols)) {
                md = data.frame() # create placeholder for market data
                symbol = niftysymbols$Symbol[i]
                kairossymbol=gsub("[^0-9A-Za-z///' ]","",symbol)
                if (file.exists(paste("../daily/", symbol, ".Rdata", sep = ""))) {
                        load(paste("../daily/", symbol, ".Rdata", sep = ""))
                        if (nrow(md) > 0) {
                                starttime = strftime(md[nrow(md), c("date")] + 1,
                                                     tz = "Asia/Kolkata",
                                                     "%Y-%m-%d %H:%M:%S")
                        }
                } else{
                        starttime = "1995-11-01 09:15:00"
                }
                temp <-
                        kGetOHLCV(
                                paste("symbol", tolower(kairossymbol), sep = "="),
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
                md <- rbind(md, temp)
		md$symbol<-symbol
                save(md,
                     file = paste("../daily/", symbol, ".Rdata", sep = "")) # save new market data to disc

        }
        # get index data
        md = data.frame() # create placeholder for market data
        symbol = "NSENIFTY"
        kairossymbol=gsub("[^0-9A-Za-z///' ]","",symbol)
        if (file.exists(paste("../daily/", symbol, ".Rdata", sep = ""))) {
                load(paste("../daily/", symbol, ".Rdata", sep = ""))
                if (nrow(md) > 0) {
                        starttime = strftime(md[nrow(md), c("date")] + 1,
                                             tz = "Asia/Kolkata",
                                             "%Y-%m-%d %H:%M:%S")
                }
        } else{
                starttime = "1995-11-01 09:15:00"
        }
        temp <-
                kGetOHLCV(
                        paste("symbol", tolower(kairossymbol), sep = "="),
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
        md <- rbind(md, temp)
        md$symbol<-symbol
        save(md,
             file = paste("../daily/", symbol, ".Rdata", sep = "")) # save new market data to disc
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
        symbol = niftysymbols$Symbol[i]
        load(paste("../daily/", symbol, ".Rdata", sep = "")) #loads md
        md <- md[md$date >= "2007-01-01",]
        md$CurrentRSI = RSI(md$settle, RSIPeriod)
        md$CurrentLTRSI=RSI(md$settle,14)
        indexRSI=RSI(index$settle,RSIPeriod)
        md$CloseAboveLongTermMA = md$settle > EMA(md$settle, MALongPeriod)
        md$LowerClose = md$settle < Ref(md$settle, -1)
        md$MultipleDayLowRSI = runSum(RSI(md$settle, 2) < EntryRSI, LowRSIBars) ==
                LowRSIBars
        AnnualizedSlope = (exp(rollapply(md$settle, 90, slope)) ^ 252) - 1
        md$AnnualizedSlope <-
                c(rep(NA, nrow(md) - length(AnnualizedSlope)), AnnualizedSlope)
        r <- rollapply(md$settle, 90, r2)
        md$r <- c(rep(NA, nrow(md) - length(r)), r)

        ####### 2. Generate Buy/Sell Arrays ##########
        md$sell = Cross(RSI(md$settle, ExitRSIPeriod), ExitRSI)

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
                Ref(md$settle,-BarsSinceFirstTrigger),
                0
        )


        SecondEntry = md$CloseAboveLongTermMA &
                md$settle < FirstTriggerPrice &
                InFirstPos &
                Ref(InFirstPos,-1) & md$AnnualizedSlope > 0 & 
                md$CurrentLTRSI >0.7

        InSecondPos = Flip(SecondEntry, md$sell)

        SecondTrigger = ExRem(InSecondPos, md$sell)

        BarsSinceSecondTrigger = BarsSince(SecondTrigger)

        SecondTriggerPrice = ifelse(
                BarsSinceSecondTrigger < BarsSinceSell,
                Ref(md$settle,-BarsSinceSecondTrigger),
                0
        )


        ThirdEntry = md$CloseAboveLongTermMA &
                md$settle < SecondTriggerPrice &
                InSecondPos &
                Ref(InSecondPos,-1) & md$AnnualizedSlope > 0 &
                md$CurrentLTRSI >0.7

        InThirdPos = Flip(ThirdEntry, md$sell)

        ThirdTrigger = ExRem(InThirdPos, md$sell)

        BarsSinceThirdTrigger = BarsSince(ThirdTrigger)

        ThirdTriggerPrice = ifelse(
                BarsSinceThirdTrigger < BarsSinceSell,
                Ref(md$settle,-BarsSinceThirdTrigger),
                0
        )


        FourthEntry = md$CloseAboveLongTermMA &
                md$settle < ThirdTriggerPrice & InThirdPos &
                Ref(InThirdPos,-1) & md$AnnualizedSlope > 0 &
                md$CurrentLTRSI >0.7

        InFourthPos = Flip(FourthEntry, md$sell)

        FourthTrigger = ExRem(InFourthPos, md$sell)

        BarsSinceFourthTrigger = BarsSince(FourthTrigger)

        FourthTriggerPrice = ifelse(
                BarsSinceFourthTrigger < BarsSinceSell,
                Ref(md$settle,-BarsSinceFourthTrigger),
                0
        )

        md$positionscore = 100 - md$CurrentRSI + (md$settle - EMA(md$settle, 20)) *
                100 / md$settle


        ####### 2. Generate Buy/Sell Arrays ##########
        md$buy = ifelse(FirstTrigger,
                        1,
                        ifelse(SecondTrigger |
                                       ThirdTrigger |
                                       FourthTrigger, 999, 0))

        md$sell = ExRem(md$sell, md$buy)

        md$short = 0

        md$cover = 0

        md$buyprice = md$settle

        md$sellprice = md$settle

        md$shortprice = md$settle

        md$coverprice = md$settle
        allmd[[i]]<-md
        signals <-
                rbind(md[, c(
                        "date",
                        "open",
                        "high",
                        "low",
                        "settle",
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
}

signals <- na.omit(signals)
signals$close <- signals$settle
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
#         load(paste("../daily/", symbol, ".Rdata", sep = "")) #loads md
#         allmd[[i]] = md[md$date > "2015-01-01",]
# }

getcontractsize <- function (x, size) {
        a <- tail(size[size$EffectiveDate < x,],1)
        return(a[length(a)]$ContractSize)

}

allequity <- list()

for (i in 1:nrow(niftysymbols)) {
        symbol = niftysymbols$Symbol[i]
        md <- allmd[[i]]
        allsize = folots[folots$Symbol == symbol,]
        size = sapply(md$date, getcontractsize, allsize)
        datesformd <- md[md$symbol == symbol, c("date")]
        allequity[[i]] = CalculatePortfolioEquityCurve(symbol, md, trades, size, 0.0002)
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
                        index = which(niftysymbols$Symbol == vsell$symbol[i])
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
                        index = which(niftysymbols$Symbol == vbuy$symbol[i])
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

#tail(allmd[[which(niftysymbols$Symbol=="BPCL")]])