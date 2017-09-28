library(forecast)
library(forecastHybrid)
library(readr)
library(gridExtra)

options(scipen=999)
par.default = par()
#remove all global variables 
rm(list = setdiff(ls(), lsf.str())) 

setwd("/mnt/A43003F9E520D223/Workplace/_b_project/demo/")
#setwd("/Volumes/_ssd_data/Analysis/")
#setwd("~/prediction")
##################################### Main ################################################
#specify number of lastest days to be used as the historical data; default is 28
historicalDays = 28 #days
ahead = 3
#length forecast horizon in hours; implying to set the 
#length of test-set as well; max is 7 days = 168 hours
horizon = min(168, ahead*historicalDays) + 12

file = "youtube4w.csv"
fileName = tools::file_path_sans_ext(basename(file))
ds = read_csv(file, col_names = "unixTime") #import dataset
ds$dateTime = as.POSIXct(ds$unixTime, origin="1970-01-01")

dsByMin = as.data.frame(table(cut(ds$dateTime, breaks = paste(1, "min", sep=" "))))
names(dsByMin)[1:2] = c("dateTime", "rate")
dsByMin$dateTime = as.POSIXct(as.vector(dsByMin$dateTime), origin="1970-01-01")

#historicalDays should equal to or less than the max number of days available in dataset
historicalDays = min(floor(nrow(dsByMin)/(60*24)),historicalDays)

################# Data Preprocessing  ####################
# dsByDay = aggregate(dsByMin$rate ~ format(dsByMin$dateTime, "%Y-%m-%d"), data=dsByMin, sum)
# names(dsByDay)[1:2] = c("dateTime", "rate")
# #suspensionDays, days that have 0 rate
# suspensionDays = dsByDay$dateTime[which(dsByDay$rate == 0)]
# 
# for(i in 1:length(suspensionDays)){
#   dsByMin = dsByMin[which(as.Date(dsByMin$dateTime) != suspensionDays[i]), ]
# }
# 
# #truncate the dataset to contain only the lastest (historicalDays*24*60) minutes
# dsByMin = dsByMin[(nrow(dsByMin) - historicalDays*24*60):nrow(dsByMin),]
# names(dsByMin)[1:2] = c("dateTime", "rate")
# 
df = aggregate(dsByMin$rate ~ format(dsByMin$dateTime, "%Y-%m-%d %H:00"), data=dsByMin, sum)
names(df)[1:2] = c("time", "rate")
df$time = as.POSIXct(df$time, origin="1970-01-01")
#str(df)
head(df)
length(which(df$rate == 0))

##########################################################
#max length of forecast horizon of the dataset in hours
maxHorizon = floor(nrow(df)/24) * ahead
#max number of days used as training set for in-sample testing
maxDay = floor((nrow(df) - maxHorizon)/24)
#The end hour of the training set 
end = maxDay*24
#The train set
tsTrain = ts(df$rate[1:end], start = 1, frequency = 24)
#The test set  
tsTest =  df$rate[(end+1):nrow(df)] 

################### Forecasting the series #####################
#simple forecast methods
averageF = meanf(tsTrain, h=horizon)
naiveF = naive(tsTrain, h=horizon)
snaiveF = snaive(tsTrain, h=horizon)
driftF = rwf(tsTrain, h=horizon, drift=TRUE)
#advanced forecast models
stlF= stlf(tsTrain, h=horizon)
etsF = forecast(ets(tsTrain), h=horizon)
arimaF = forecast(auto.arima(tsTrain), h=horizon)
hybridF = forecast(hybridModel(tsTrain), h=horizon)

###################### Accuracy Test ###########################
#store all forecasts objects into vector 'forecastMethods'
forecastMethods = list(averageF, naiveF, snaiveF, driftF, stlF, etsF, arimaF, hybridF)
#errorTable: matrix to examine the accuracy of all forcasts model against 4 error methods
errorTable = matrix(nrow = 0, ncol = 4)
colnames(errorTable) = c("RMSE","MAE","MAPE","MASE")
for(method in forecastMethods) {
  errorTable = rbind(errorTable, c(as.vector(accuracy(method, tsTest)[2,c(2,3,5,6)])))
}
rownames(errorTable) = c("Average", "Naive", "S. Naive", "Drift", "STL", "ETS", "ARIMA", "Hybrid")

########## Ranking and selecting the best forecast method  ##########
#compare the accuracy of forecasting methods and select the best one
winner = ""   #method selected that is the best in term of minimum errors
winnerEval = vector("character", length = ncol(errorTable))

#search the min values of each accuracy method (the cols) to find the best prediction method
for(j in 1:ncol(errorTable)){
  #find the min value in each accuracy methods and output the name of the corresponding prediction method.
  winnerEval[j] = ifelse("NaN" %in% errorTable[,j], "", rownames(errorTable)[which(errorTable[,j] == min(errorTable[,j]))]) 
}

#one method is the dominator
if(length(unique(winnerEval)) == 1){
  winner = winnerEval[1] 
} else{ 
  #Here is when several methods have minimum at different error measurements
  #Sort methods according to theirs number of winning.
  #Here we assume all error methods has the same priority/weight, but one can also change their priority.
  #Setting priority is important, since it decides which forecast method to be used for the broker
  #Ranking forecasts method by their occurences in 'winnerEval'
  ranking = sort(summary(as.factor(winnerEval)), decreasing=TRUE)
  #if 4 different forecast methods wins in every of the 4 error methods
  #method winning in MAPE is prioritized
  if(length(ranking) == 4){
    winner = winnerEval[3] 
  } else{
    winner = names(ranking)[1]}
}

tsEntire = ts(df$rate, start = 1, frequency = 24)
result = switch(winner,
             Average = {(meanf(tsEntire, h=horizon))},
             Naive = {naive(tsEntire, h=horizon)},
             Snaive = {(snaive(tsEntire, h=horizon))},
             Drift = {(rwf(tsEntire, h=horizon, drift=TRUE))},
             STL = {(stlf(tsEntire, h=horizon))},
             ETS = {(forecast(ets(tsEntire), h=horizon))},
             ARIMA = {(forecast(auto.arima(tsEntire), h=horizon))},
             Hybrid = {(forecast(hybridModel(tsEntire), h=horizon))},
             {print("Error occured!")})
result$model
result$mean

##### ##### ##### ##### Export table of error measurements to  pdfs ##### ##### ##### #####
# pdf(paste0(reportPrefix, "_accuracy.pdf"), width = 6.5, height = 2.5)
# grid.table(errorTable)
# dev.off()

# line = paste(Sys.time(),winner,sep = ",")
# reportMethodSelection = paste0("reports/",fileName,"/",fileName,"_methodSelect.csv")
# if(file.exists(reportMethodSelection)) {
#   write(line,file=reportMethodSelection, append=TRUE)
# } else {
#   file.create(reportMethodSelection, showWarnings = FALSE)
#   write("Date,MethodSelected",file=reportMethodSelection, append=TRUE)
#   write(line,file=reportMethodSelection, append=TRUE)
# }

