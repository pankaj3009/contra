library(TTR)
library(rredis)
library(log4r)
library(RTrade)
library(RcppRoll)
library(zoo)
library(plyr)
library(RQuantLib)
options(scipen=999)

args.commandline=commandArgs(trailingOnly=TRUE)
if(length(args.commandline)>0){
        args<-args.commandline
}

# args<-c("2","swing01","3")
# args[1] is a flag for model building. 0=> Build Model, 1=> Generate Signals in Production 2=> Backtest and BootStrap 4=>Save BOD Signals to Redis
# args[2] is the strategy name
# args[3] is the redisdatabase
redisConnect()
redisSelect(1)
if(length(args)>1){
        static<-redisHGetAll(toupper(args[2]))
}else{
        static<-redisHGetAll("CONTRA-LLO")
}

newargs<-unlist(strsplit(static$args,","))
if(length(args)<=1 && length(newargs>1)){
        args<-newargs
}
redisClose()


kWriteToRedis <- as.logical(static$WriteToRedis)
kGetMarketData<-as.logical(static$GetMarketData)
kUseSystemDate<-as.logical(static$UseSystemDate)
kDataCutOffBefore<-static$DataCutOffBefore
kBackTestStartDate<-static$BackTestStartDate
kBackTestEndDate<-static$BackTestEndDate
kFNODataFolder <- static$FNODataFolder
kNiftyDataFolder <- static$NiftyDataFolder
kTimeZone <- static$TimeZone
kBrokerage<-as.numeric(static$SingleLegBrokerageAsPercentOfValue)/100
kExchangeMargin<-as.numeric(static$ExchangeMargin)
kPerContractBrokerage=as.numeric(static$SingleLegBrokerageAsValuePerContract)
kMaxContracts=as.numeric(static$MaxContracts)
kHomeDirectory=static$HomeDirectory
kLogFile=static$LogFile
kExclusionsFile=static$ExclusionsFile
setwd(kHomeDirectory)
strategyname = args[2]
redisDB = args[3]
kBackTestEndDate=strftime(adjust("India",as.Date(kBackTestEndDate, tz = kTimeZone),bdc=2),"%Y-%m-%d")
kBackTestStartDate=strftime(adjust("India",as.Date(kBackTestStartDate, tz = kTimeZone),bdc=0),"%Y-%m-%d")


logger <- create.logger()
logfile(logger) <- kLogFile
level(logger) <- 'INFO'
levellog(logger, "INFO", "Starting BOD Scan")


###### BACKTEST ##############

###### Load Data #############

redisConnect()
redisSelect(2)
#update splits
a<-unlist(redisSMembers("splits")) # get values from redis in a vector
tmp <- (strsplit(a, split="_")) # convert vector to list
k<-lengths(tmp) # expansion size for each list element
allvalues<-unlist(tmp) # convert list to vector
splits <- data.frame(date=1:length(a), symbol=1:length(a),oldshares=1:length(a),newshares=1:length(a),reason=rep("",length(a)),stringsAsFactors = FALSE)
for(i in 1:length(a)) {
  for(j in 1:k[i]){
    runsum=cumsum(k)[i]
    splits[i, j] <- allvalues[runsum-k[i]+j]
  }
}
splits$date=as.POSIXct(splits$date,format="%Y%m%d",tz="Asia/Kolkata")

#update symbol change
a<-unlist(redisSMembers("symbolchange")) # get values from redis in a vector
tmp <- (strsplit(a, split="_")) # convert vector to list
k<-lengths(tmp) # expansion size for each list element
allvalues<-unlist(tmp) # convert list to vector
symbolchange <- data.frame(date=rep("",length(a)), key=rep("",length(a)),newsymbol=rep("",length(a)),stringsAsFactors = FALSE)
for(i in 1:length(a)) {
  for(j in 1:k[i]){
    runsum=cumsum(k)[i]
    symbolchange[i, j] <- allvalues[runsum-k[i]+j]
  }
}
symbolchange$date=as.POSIXct(symbolchange$date,format="%Y%m%d",tz="Asia/Kolkata")
symbolchange$key = gsub("[^0-9A-Za-z/-]", "", symbolchange$key)
symbolchange$newsymbol = gsub("[^0-9A-Za-z/-]", "", symbolchange$newsymbol)
redisClose()

niftysymbols <-
        createIndexConstituents(2, "nifty50", threshold = "2015-01-01")
folots <- createFNOSize(2, "contractsize", threshold = "2015-01-01")

invalidsymbols = numeric()
endtime = format(Sys.time(), format = "%Y-%m-%d %H:%M:%S")
if(!is.null(kExclusionsFile)){
        exclusions<-unlist(read.csv(kExclusionsFile,header=FALSE,stringsAsFactors = FALSE))    
        indicestoremove=match(exclusions,niftysymbols$symbol)
        niftysymbols<-niftysymbols[-indicestoremove,]
}

if (kGetMarketData) {
        for (i in 1:nrow(niftysymbols)) {
                # foreach (i =1:nrow(niftysymbols),.packages="RTrade") %dopar% {
                md = data.frame() # create placeholder for market data
                symbol = niftysymbols$symbol[i]
                kairossymbol = symbol
                if (file.exists(paste(kNiftyDataFolder, symbol, ".Rdata", sep = ""))) {
                        load(paste(kNiftyDataFolder, symbol, ".Rdata", sep = ""))
                        if (nrow(md) > 0) {
                                starttime = strftime(md[nrow(md), c("date")] + 1,
                                                     tz = "Asia/Kolkata",
                                                     "%Y-%m-%d %H:%M:%S")
                        } else{
                                starttime = "1995-11-01 09:15:00"
                        }
                } else{
                        starttime = "1995-11-01 09:15:00"
                }
                temp <-
                        kGetOHLCV(
                                paste("symbol", tolower(kairossymbol), sep = "="),
                                df = md,
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
                md <- temp
                md$symbol <- symbol
                save(md,
                     file = paste(kNiftyDataFolder, symbol, ".Rdata", sep = "")) # save new market data to disc

        }
        #stopCluster(cl)

        # get index data
        md = data.frame() # create placeholder for market data
        symbol = "NSENIFTY"
        #kairossymbol=gsub("[^0-9A-Za-z///' ]","",symbol)
        kairossymbol = symbol
        if (file.exists(paste(kNiftyDataFolder, symbol, ".Rdata", sep = ""))) {
                load(paste(kNiftyDataFolder, symbol, ".Rdata", sep = ""))
                if (nrow(md) > 0) {
                        starttime = strftime(md[nrow(md), c("date")] + 1,
                                             tz = "Asia/Kolkata",
                                             "%Y-%m-%d %H:%M:%S")
                } else{
                        starttime = "1995-11-01 09:15:00"
                }
        } else{
                starttime = "1995-11-01 09:15:00"
        }
        temp <-
                kGetOHLCV(
                        paste("symbol", tolower(kairossymbol), sep = "="),
                        df = md,
                        start = starttime,
                        end = endtime,
                        timezone = "Asia/Kolkata",
                        name = "india.nse.index.s4.daily",
                        ts = c("open",
                               "high",
                               "low",
                               "settle",
                               "close",
                               "volume"),
                        aggregators = c("first",
                                        "max",
                                        "min",
                                        "last",
                                        "last",
                                        "sum"),
                        aValue = "1",
                        aUnit = "days",
                        splits = splits,
                        symbolchange = symbolchange

                )
        #md <- rbind(md, temp)
        md <- temp
        md$symbol <- symbol
        save(md,
             file = paste(kNiftyDataFolder, symbol, ".Rdata", sep = "")) # save new market data to disc
        index <- md


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
        if (file.exists(paste(kNiftyDataFolder, symbol, ".Rdata", sep = ""))) {
                load(paste(kNiftyDataFolder, symbol, ".Rdata", sep = "")) #loads md
                md <- md[md$date >= kDataCutOffBefore, ]
                if (nrow(md) > MALongPeriod) {
                        md$CurrentRSI = RSI(md$asettle, RSIPeriod)
                        md$eligible = ifelse(
                                as.Date(md$date) >= niftysymbols[i, c("startdate")] &
                                        as.Date(md$date) <= niftysymbols[i, c("enddate")],
                                1,
                                0
                        )
                        md$CurrentLTRSI = RSI(md$asettle, 14)
                        md$CloseAboveLongTermMA = md$asettle > EMA(md$asettle, MALongPeriod)
                        md$LowerClose = md$asettle < Ref(md$asettle,-1)
                        md$MultipleDayLowRSI = runSum(RSI(md$asettle, 2) < EntryRSI,
                                                      LowRSIBars) ==
                                LowRSIBars
                        pointslope=rollapply(md$asettle, 90, slope)
                        md$pointslope= c(rep(NA,nrow(md) - length(pointslope)),pointslope)
                        AnnualizedSlope = (exp(rollapply(
                                md$asettle, 90, slope
                        )) ^ 252) - 1
                        md$AnnualizedSlope <-
                                c(rep(
                                        NA,
                                        nrow(md) - length(AnnualizedSlope)
                                ),
                                AnnualizedSlope)
                        r <- rollapply(md$asettle, 90, r2)
                        md$r <- c(rep(NA, nrow(md) - length(r)), r)
                        ####### 2. Generate Buy/Sell Arrays ##########
                        md$sell = Cross(RSI(md$asettle, ExitRSIPeriod),
                                        ExitRSI)

                        BarsSinceSell = BarsSince(md$sell)

                        md$firstentry = md$CloseAboveLongTermMA &
                                md$MultipleDayLowRSI &
                                md$AnnualizedSlope > 0 & md$r > 0.7 &
                                md$CurrentLTRSI > 0.7
                        InFirstPos = Flip(md$firstentry, md$sell)

                        FirstTrigger = ExRem(InFirstPos, md$sell)

                        BarsSinceFirstTrigger = BarsSince(FirstTrigger)
                        FirstTriggerPrice = ifelse(
                                BarsSinceFirstTrigger < BarsSinceSell,
                                Ref(md$asettle, -BarsSinceFirstTrigger),
                                0
                        )


                        SecondEntry = md$CloseAboveLongTermMA &
                                md$asettle < FirstTriggerPrice &
                                InFirstPos &
                                Ref(InFirstPos, -1) & md$AnnualizedSlope > 0 &
                                md$CurrentLTRSI > 0.7

                        InSecondPos = Flip(SecondEntry, md$sell)

                        SecondTrigger = ExRem(InSecondPos, md$sell)

                        BarsSinceSecondTrigger = BarsSince(SecondTrigger)

                        SecondTriggerPrice = ifelse(
                                BarsSinceSecondTrigger < BarsSinceSell,
                                Ref(md$asettle, -BarsSinceSecondTrigger),
                                0
                        )


                        ThirdEntry = md$CloseAboveLongTermMA &
                                md$asettle < SecondTriggerPrice &
                                InSecondPos &
                                Ref(InSecondPos, -1) & md$AnnualizedSlope > 0 &
                                md$CurrentLTRSI > 0.7

                        InThirdPos = Flip(ThirdEntry, md$sell)

                        ThirdTrigger = ExRem(InThirdPos, md$sell)

                        BarsSinceThirdTrigger = BarsSince(ThirdTrigger)

                        ThirdTriggerPrice = ifelse(
                                BarsSinceThirdTrigger < BarsSinceSell,
                                Ref(md$asettle, -BarsSinceThirdTrigger),
                                0
                        )


                        FourthEntry = md$CloseAboveLongTermMA &
                                md$asettle < ThirdTriggerPrice & InThirdPos &
                                Ref(InThirdPos, -1) & md$AnnualizedSlope > 0 &
                                md$CurrentLTRSI > 0.7

                        InFourthPos = Flip(FourthEntry, md$sell)

                        FourthTrigger = ExRem(InFourthPos, md$sell)

                        BarsSinceFourthTrigger = BarsSince(FourthTrigger)

                        FourthTriggerPrice = ifelse(
                                BarsSinceFourthTrigger < BarsSinceSell,
                                Ref(md$asettle, -BarsSinceFourthTrigger),
                                0
                        )

                        md$positionscore = 100 - md$CurrentRSI + (md$asettle - EMA(md$asettle, 20)) *
                                100 / md$asettle


                        ####### 2. Generate Buy/Sell Arrays ##########
                        md$buy = ifelse(
                                FirstTrigger,
                                1,
                                ifelse(
                                        SecondTrigger |
                                                ThirdTrigger |
                                                FourthTrigger,
                                        999,
                                        0
                                )
                        )
                        md$buy = ifelse(md$eligible == 1, md$buy, 0)
                        md$sell = ExRem(md$sell, md$buy)

                        md$short = 0

                        md$cover = 0

                        md$buyprice = md$settle

                        md$sellprice = md$settle

                        md$shortprice = md$settle

                        md$coverprice = md$settle
                        md <-  md[md$date >= kBackTestStartDate & md$date <= kBackTestEndDate, ]
                        allmd[[i]] <- md
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
                } else{
                        invalidsymbols <- c(invalidsymbols, i)
                }
        }
}
#niftysymbols<-niftysymbols[-invalidsymbols,]

signals <- na.omit(signals)
signals$aclose <- signals$asettle
dates <- unique(signals[order(signals$date), c("date")])
a <- ProcessPositionScore(signals, 5, dates)
# Generate consolidated buy/sell
processedsignals <- ApplyStop(a, rep(10000000, nrow(a)))
processedsignals <- processedsignals[order(processedsignals$date), ]
processedsignals$currentmonthexpiry <- as.Date(sapply(processedsignals$date, getExpiryDate), tz = kTimeZone)
nextexpiry <- as.Date(sapply(
         as.Date(processedsignals$currentmonthexpiry + 20, tz = kTimeZone),
         getExpiryDate), tz = kTimeZone)
processedsignals$entrycontractexpiry <- as.Date(ifelse(
        businessDaysBetween("India",as.Date(processedsignals$date, tz = kTimeZone),processedsignals$currentmonthexpiry) <= 3,
        nextexpiry,processedsignals$currentmonthexpiry),tz = kTimeZone)

processedsignals<-getClosestStrikeUniverse(processedsignals,kFNODataFolder,kNiftyDataFolder,kTimeZone)
signals<-processedsignals
multisymbol<-function(uniquesymbols,df,fnodatafolder,equitydatafolder){
        out=NULL
        for(i in 1:length(unique(df$symbol))){
                temp<-optionTradeSignalsLongOnly(df[df$symbol==unique(df$symbol)[i],],kFNODataFolder,kNiftyDataFolder,rollover=TRUE)
                out<-rbind(out,temp)
        }
        out
}

optionSignals<-multisymbol(unique(signals$symbol),signals,fnodatafolder,equitydatafolder)
optionSignals<-optionSignals[with(optionSignals,order(date,symbol,buy,sell)),]

#signals[intersect(grep("ULTRACEMCO",signals$symbol),union(which(signals$buy>=1),which(signals$sell>=1))),]

trades <- GenerateTrades(optionSignals)
trades <- trades[order(trades$entrytime), ]

getcontractsize <- function (x, size) {
        a <- size[size$startdate <= as.Date(x) & size$enddate >= as.Date(x), ]
        if (nrow(a) > 0) {
                a <- head(a, 1)
        }
        if (nrow(a) > 0) {
                return(a$contractsize)
        } else
                return(0)

}

if(nrow(trades)>0){
        trades$size=NULL
        novalue=strptime(NA_character_,"%Y-%m-%d")
        for(i in 1:nrow(trades)){
                symbolsvector=unlist(strsplit(trades$symbol[i],"_"))
                allsize = folots[folots$symbol == symbolsvector[1], ]
                trades$size[i]=getcontractsize(trades$entrytime[i],allsize)
                if(as.numeric(trades$exittime[i])==0){
                        trades$exittime[i]=novalue
                }
        }
        
        trades$brokerage <- 2*kPerContractBrokerage / (trades$entryprice*trades$size)
        trades$netpercentprofit <- trades$percentprofit - trades$brokerage
        trades$absolutepnl<-NA_real_
        BizDayBacktestEnd=adjust("India",min(Sys.Date(),
                                             as.Date(kBackTestEndDate, tz = kTimeZone)),bdc=2)
        for (t in 1:nrow(trades)) {
                expirydate = as.Date(unlist(strsplit(trades$symbol[t], "_"))[3], "%Y%m%d", tz =
                                             kTimeZone)
                if (trades$exitprice[t]==0 &&
                    (
                            (expirydate > min(Sys.Date(),
                                              as.Date(kBackTestEndDate, tz = kTimeZone)))
                    )){
                        symbolsvector=unlist(strsplit(trades$symbol[t],"_"))
                        load(paste(kFNODataFolder,symbolsvector[3],"/", trades$symbol[t], ".Rdata", sep = ""))
                        index=which(as.Date(md$date,tz=kTimeZone)==BizDayBacktestEnd)
                        if(length(index)==1){
                                trades$exitprice[t] = md$settle[index]
                        }else{
                                trades$exitprice[t] = tail(md$settle,1)
                        }
                }
                trades$absolutepnl[t] = (trades$exitprice[t] - trades$entryprice[t] -
                                                 (2*kPerContractBrokerage / trades$size[t])) * trades$size[t]
        }
        
        
        trades$absolutepnl[which(is.na(trades$absolutepnl))]<-0
}


#trades$pnl<-(trades$exitprice-trades$entryprice)*trades$size

########### SAVE SIGNALS TO REDIS #################

entrysize <- 0
exitsize <- 0

if (kUseSystemDate) {
  today = Sys.Date()
} else{
  today = advance("India",dates=Sys.Date(),n=-1,timeUnit = 0,bdc=2)
}
yesterday=advance("India",dates=today,n=-1,timeUnit = 0,bdc=2)

entrycount = which(as.Date(trades$entrytime,tz=kTimeZone) == today)
exitcount = which(as.Date(trades$exittime,tz=kTimeZone) == today)

if (length(exitcount) > 0 && kWriteToRedis && args[1]==1) {
  redisConnect()
  redisSelect(args[3])
  out <- trades[which(as.Date(trades$exittime,tz=kTimeZone) == today),]
  uniquesymbols=NULL
  for (o in 1:nrow(out)) {
    if(length(grep(out[o,"symbol"],uniquesymbols))==0){
      uniquesymbols[length(uniquesymbols)+1]<-out[o,"symbol"]
      startingposition = GetCurrentPosition(out[o, "symbol"], trades,position.on=yesterday,trades.till=yesterday)
      redisString = paste(out[o, "symbol"],
                          abs(startingposition),
                          ifelse(startingposition>0,"SELL","COVER"),
                          0,
                          abs(startingposition),
                          sep = ":")
      redisRPush(paste("trades", args[2], sep = ":"),
                 charToRaw(redisString))
      levellog(logger,
               "INFO",
               paste(args[2], redisString, sep = ":"))
    }
  }
  redisClose()
}


if (length(entrycount) > 0 && kWriteToRedis && args[1]==1) {
  redisConnect()
  redisSelect(args[3])
  out <- trades[which(as.Date(trades$entrytime,tz=kTimeZone) == today),]
  uniquesymbols=NULL
  for (o in 1:nrow(out)) {
    if(length(grep(out[o,"symbol"],uniquesymbols))==0){
      uniquesymbols[length(uniquesymbols)+1]<-out[o,"symbol"]
      startingposition = GetCurrentPosition(out[o, "symbol"], trades,trades.till=yesterday,position.on = today)
      todaytradesize=GetCurrentPosition(out[o, "symbol"], trades)-startingposition
      redisString = paste(out[o, "symbol"],
                          abs(todaytradesize),
                          ifelse(todaytradesize>0,"BUY","SHORT"),
                          0,
                          abs(startingposition),
                          sep = ":")
      redisRPush(paste("trades", args[2], sep = ":"),
                 charToRaw(redisString))
      levellog(logger,
               "INFO",
               paste(args[2], redisString, sep = ":"))
      
    }
    
  }
  redisClose()
}


############ METRICS ##################
if(nrow(trades)>0){
        symbol=niftysymbols$symbol[1]
        datesinscope=numeric()
        if (file.exists(paste(kNiftyDataFolder, symbol, ".Rdata", sep = ""))) {
                load(paste(kNiftyDataFolder, symbol, ".Rdata", sep = ""))
                datesinscope=md[md$date>kBackTestStartDate & md$date<=kBackTestEndDate,c("date")]
                pnl<-data.frame(bizdays=as.Date(datesinscope,tz=kTimeZone),realized=0,unrealized=0,brokerage=0)
                cleansedTrades<-trades[!is.na(trades$entryprice),]
                cumpnl<-CalculateDailyPNL(cleansedTrades,pnl,kFNODataFolder,kPerContractBrokerage/cleansedTrades$size,deriv=TRUE)
        }
        
        DailyPNL <-  (cumpnl$realized + cumpnl$unrealized-cumpnl$brokerage) - Ref(cumpnl$realized + cumpnl$unrealized-cumpnl$brokerage, -1)
        DailyPNL <- ifelse(is.na(DailyPNL), 0, DailyPNL)
        Capital<-20*500000*kExchangeMargin
        DailyReturn<-DailyPNL/Capital
        df <- data.frame(time = as.Date(datesinscope,tz=kTimeZone), return = DailyReturn)
        df <- read.zoo(df)
        sharpeRatio <-  sharpe(DailyReturn)
        print(paste("sharpe:", sharpeRatio, sep = ""))
        
        
        return<-sum(trades$absolutepnl) * 100 / Capital
        print(paste("Annual Return:",return))
        print(paste("winratio:", sum(trades$absolutepnl > 0) / nrow(trades), sep =""))
        
        if(nrow(cumpnl)>1){
                cumpnl$group <- strftime(cumpnl$bizdays, "%Y%m")
                cumpnl$dailypnl<-DailyPNL
                dd.agg <- aggregate(dailypnl ~ group, cumpnl, FUN = sum)
                print(paste("Average Loss in Losing Month:",mean(dd.agg[which(dd.agg$dailypnl<0),'dailypnl']),sep=""))
                print(paste("Percentage Losing Months:",sum(dd.agg$dailypnl<0)/nrow(dd.agg)))
        }
        
}
