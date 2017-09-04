library(forecast)
library(readr)
#library(xts)
#library(tseries)
library(gridExtra)
library(imputeTS)
library(rJava)  #sudo R CMD javareconf
options(scipen=999)
par.default = par()
#options(error = browser())

#1. try to use tsclean() to eliminate 0 rate interval
#2. edit code to use with JRI

setwd("/mnt/A43003F9E520D223/Workplace/_data")
##################################### Main ################################################
timeLength = 28 #days   #specify the last #timeLength days backwards to be used as the historical data
testSet = 3 #days     #specify how many days in #timeLength to be used as test set; implying to set forecast horizon as well
ahead = testSet * 24 #length forecast horizon in hours
cutBy = 60 #mins - each time intervals has 60 mins 
freqs = c(24)

day=timeLength

for(freq in freqs){
  ################# Data Preprocessing  ####################
  file = "tsfiles/youtube.csv"
  fileName = tools::file_path_sans_ext(basename(file))
  ds = read_csv(file, col_names = "unixTime") #dataset
  ds$dateTime = as.POSIXct(ds$unixTime, origin="1970-01-01")
  ds = ds[nrow(ds):1,]
  
  df = as.data.frame(table(cut(ds$dateTime, breaks = paste(cutBy, "mins", sep=" "))))
  df = df[(nrow(df) - timeLength*24):nrow(df),] 
  names(df)[1:2] = c("time", "rate")
  df$time = as.POSIXct(df$time, origin="1970-01-01")
  
  ################# Prepare for Report  ####################
  ifelse(!dir.exists(file.path("reports")), dir.create(file.path("reports")), FALSE)
  ifelse(!dir.exists(file.path("reports/", fileName)), dir.create(file.path("reports/", fileName)), FALSE)
  reportPrefix = paste0("reports/",fileName,"/",fileName,"_",format(Sys.time(), "%d%m%y_%k-%M"))
  
  #this matrix has its cols as accuracy methods e.g. MAE, RMSE,... and its rows as prediction methods e.g. ETS, ARIMA
  evalMatrix  = matrix(nrow = 0, ncol = 6)
  
  last = (day*24)
  ts = ts(df$rate[1:last], start = 1, frequency = freq)
  ts[which(ts == 0)] = NA
  if (sum(is.na(ts)) != 0){
    ts = tsclean(df$rate[1:last], replace.missing = TRUE)
    #ts = na.kalman(ts, model = "ets", smooth = true)
    #plot(ts)
    #dev.off()
  }
  tsTrain = ts(df$rate[1:(last-ahead)], start = 1, frequency = freq) #training dataset
  tsTest =  ts[(last-ahead+1):last] #testing dataset    
  
  ################### Predicting the series #####################
  
  fAverage = meanf(tsTrain, h=ahead)
  fNaive = naive(tsTrain, h=ahead)
  fSnaive = snaive(tsTrain, h=ahead)
  fDrift = rwf(tsTrain, h=ahead, drift=TRUE)
  fETS = forecast(ets(tsTrain), h=ahead)
  fARIMA = forecast(auto.arima(tsTrain), h=ahead)
  
  ################### Accuracy Test  ################### 
  
  forecastMethods = list(fAverage, fNaive, fSnaive, fDrift, fETS, fARIMA)
  colnames(evalMatrix) = c("RMSE","MAE","MAPE","MASE", "Mean 80-IW", "Mean 95-IW")
  for(method in forecastMethods) {
    evalMatrix = rbind(evalMatrix, c(as.vector(accuracy(method, tsTest)[2,c(2,3,5,6)]),   
                                     round(sum(method$upper[,1] - method$lower[,1])/ahead, 4),
                                     round(sum(method$upper[,2] - method$lower[,2])/ahead, 4)) )
  }
  rownames(evalMatrix) = c("Average", "Naive", "S. Naive", "Drift", "ETS", "ARIMA")
  evalMatrix = round(evalMatrix, 4)
  
  ###################### Plotting #########################
  pdf(paste0(reportPrefix,"_plot.pdf"), width = 8.5, height = 4.5)  
  plot(ts, main=paste(fileName,format(Sys.time(), "%a_%b%d_%X%Y")), xaxt="n", xlab = "")
  lines(fSnaive$mean, col=3, lwd=1)
  lines(fDrift$mean,col=5, lwd=1)
  lines(fARIMA$mean, col=2, lwd=1)
  lines(fETS$mean,col=4, lwd=1)
  
  legend("topright",lty=1,col=c(3,5,2,4), lwd=2, 
         legend=c("Seasonal naive method","Drift method","ARIMA","ETS"))
  ###################### Adjusting axes #########################
  #This if/else should be adapted to changes to the 'freqs' vector
  #Set ticks and labels for axes of plots
  if(day <= 7){
    lab = c(df$time[1], df$time[seq(12, (day*24),by=12)])
    ticks = c(1,(as.numeric(lab[2:length(lab)]) - as.numeric(lab[2]))/(3600*freq)+2)
  } else {
    lab = c(df$time[1], df$time[seq(24, (day*24),by=24)])
    ticks = c(1,(as.numeric(lab[2:length(lab)]) - as.numeric(lab[2]))/(3600*freq)+2)
  }
  
  axis(1, at=ticks, 
       labels = strftime(lab, format="%a %d/%m\n%H:%M"), 
       las=1, cex.axis = 0.6)
  abline(v=ticks, col="gray80", lty=5)
  dev.off()
  
  ##########  ##########  ########## Evaluating and Selecting the bes Prediction method ##########  ##########  ##########
  #compare the accuracy of forecasting methods and select the best one
  winner = ""   #method selected that is the best in term of minimum errors
  winnerEval = vector("character", length = ncol(evalMatrix))
  
  #search the min values of each accuracy method (the cols) to find the best prediction method
  for(j in 1:ncol(evalMatrix)){
    #find the min value in each accuracy methods and output the name of the corresponding prediction method.
    winnerEval[j] = ifelse("NaN" %in% evalMatrix[,j], "", rownames(evalMatrix)[which(evalMatrix[,j] == min(evalMatrix[,j]))]) 
  }
  
  #one method is the dominator
  if(length(unique(winnerEval)) == 1 || (length(unique(winnerEval)) == 2 && "" %in% winnerEval)){
    winner[1] = winnerEval[1]
  }
  #several methods have minimum at different error measurements
  else { 
    #Sort methods according to theirs number of winning .
    #Here we assume all error methods has the same priority/weight, but one can also change their priority.
    #Setting priority is important, since it will affect which forecast output to be used by the broker
    ranking = sort(summary(as.factor(winnerEval)), decreasing=TRUE)
    winner[1]  = ranking[1]
    #winners=""
    #for(i in 1:length(ranking)){
    #curr = paste0(names(ranking[i]),"*",paste0(which(winnerEval == names(ranking[i])),collapse = ","))
    #winners = ifelse(i == 1, curr, paste0(winners,"\n",curr))
    #}
    #methodSelection[fileName, day] <<- winners    #the best method stands at index 1, second best at 2, and so on.
  }
  
  res = switch(winner,
         Average = {(fAverage)},
         Naive = {(fNaive)},
         Snaive = {(fSnaive)},
         Drift = {(fDrift)},
         ETS = {(fETS)},
         ARIMA = {(fARIMA)},
         {print("Error occured!")}
  )
  #return(res)
  #export table of error measurements
  pdf(paste0(reportPrefix, "_accuracy.pdf"), width = 6.5, height = 2.5)
  grid.table(evalMatrix)
  dev.off()
  
  line = paste(Sys.time(),winner,sep = ",")
  reportMethodSelection = paste0("reports/",fileName,"/",fileName,"_methodSelect.csv")
  if(file.exists(reportMethodSelection)) {
    write(line,file=reportMethodSelection, append=TRUE)
  } else {
    file.create(reportMethodSelection, showWarnings = FALSE)
    write("Date,MethodSelected",file=reportMethodSelection, append=TRUE)
    write(line,file=reportMethodSelection, append=TRUE)
  }
}
