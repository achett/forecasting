library(tidyverse)
library(dplyr)
library(ranger)
library(tidymodels)
library(hts)
library(prophet)
library(forecast)
library(forecastHybrid)
library(data.table)
library(sqldf)
library(zoo)
library(lubridate)
library(RcppRoll)
library(ggthemes)
library(plyr)
library(Metrics)
library(doParallel)  
library(future)  
library(tseries)
library(stats)
library(grid)
library(gridExtra)
library(raster)
library(lubridate)
library(xlsx)
library(officer)
library(magrittr)
library(Hmisc)

'%!in%' <- function(x,y)!('%in%'(x,y))

multiplot <- function(..., plotlist=NULL, file, cols=1, layout=NULL) {
  require(grid)

  # Make a list from the ... arguments and plotlist
  plots <- c(list(...), plotlist)

  numPlots = length(plots)

  # If layout is NULL, then use 'cols' to determine layout
  if (is.null(layout)) {
    # Make the panel
    # ncol: Number of columns of plots
    # nrow: Number of rows needed, calculated from # of cols
    layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),
                    ncol = cols, nrow = ceiling(numPlots/cols))
  }

 if (numPlots==1) {
    print(plots[[1]])

  } else {
    # Set up the page
    grid.newpage()
    pushViewport(viewport(layout = grid.layout(nrow(layout), ncol(layout))))

    # Make each plot, in the correct location
    for (i in 1:numPlots) {
      # Get the i,j matrix positions of the regions that contain this subplot
      matchidx <- as.data.frame(which(layout == i, arr.ind = TRUE))

      print(plots[[i]], vp = viewport(layout.pos.row = matchidx$row,
                                      layout.pos.col = matchidx$col))
    }
  }
}

run_ets<-function(i,data, id, periodsToForecast, start_date, opt_criteria, run_type){

    thisTag<-data[i,id]
    # Arrange time series
    x<-data$data[[i]]%>%arrange(ds)

    x<-x%>%dplyr::rename(y=value)

    aTs<-ts(x$y,start=start_date,freq=12)

    model = ets(aTs,opt.crit=opt_criteria)

        fct<-forecast(model,h=periodsToForecast)
        ef1<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(model$fitted))),frac=0),yhat=as.numeric(model$fitted))%>%pivot_wider(names_from=ds,values_from=yhat)
        
        ef2<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(model$x))),frac=0),yhat=as.numeric(model$x))%>%pivot_wider(names_from=ds,values_from=yhat)
        
        ef3<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(fct$mean))),frac=0),yhat=as.numeric(fct$mean))%>%pivot_wider(names_from=ds,values_from=yhat)
        ef1=cbind(ef1, ef3)
        ef1$run_type=run_type
        ef2$run_type='actuals'
        ef=rbind.fill(ef1,ef2)
    
    ef$rmse=forecast::accuracy(model)[2]
    ef$mape=forecast::accuracy(model)[5]

    ef$tag=thisTag[[1]]

    ef[ef<0] <- 0

    ef$confidence=mean((fct$upper-fct$mean)/fct$mean)

  return(ef)

}

run_prophet<-function(i,data, id, periodsToForecast, start_date, run_type){
    print("id")
    thisTag<-data[i,id]
    # Arrange time series
    x<-data$data[[i]]%>%arrange(ds)

    x<-x%>%dplyr::rename(y=value)

    capNum<-quantile(x$y,.98,na.rm=TRUE)*2
    floorNum<-min(0,quantile(x$y,.01,na.rm=T))
    thisTs<-x%>%mutate(cap=capNum,floor=floorNum)

    mod<-prophet(thisTs)

    future <- make_future_dataframe(mod, freq = "month", periods = periodsToForecast)
    thisForecast<-predict(mod,future)


        pf1<-head(thisForecast, NROW(thisForecast))%>%dplyr::select(ds,yhat)%>%pivot_wider(names_from=ds,values_from=yhat)
        pf1$run_type=run_type
        pf2<-mod$history%>%dplyr::select(ds,y)%>%pivot_wider(names_from=ds,values_from=y)
        pf2$run_type='actuals'
        pf=rbind.fill(pf1,pf2)
        
    metrics=pf[ , colSums(is.na(pf)) == 0]
    print(metrics)
    pf$rmse=rmse(as.numeric(head(c(metrics[1,]), -1)), as.numeric(head(c(metrics[2,]), -1)))
    pf$mape=mape(as.numeric(head(c(metrics[1,]), -1)), as.numeric(head(c(metrics[2,]), -1)))*100


    pf$tag=thisTag[[1]]

    pf[pf<0] <- 0

    pf$confidence=mean((thisForecast$yhat_upper-thisForecast$yhat)/thisForecast$yhat)

  return(pf)

}

run_ensemble<-function(i,data, id, periodsToForecast, start_date, run_type){

    thisTag<-data[i,id]

    #Prep time series
    x<-data$data[[i]]%>%arrange(ds)
    x<-x%>%dplyr::rename(y=value)
    x$ds=as.Date(x$ds)
    ts <- ts(x$y, start=start_date, frequency=12)
    ts=na.remove(ts)

    #Fit models
    #hm1=thetam(ts)
    hm1 <- hybridModel(y = ts, models = "aefst")
    hForecast <- forecast(hm1, h = periodsToForecast, level=.95) 
    

        fct1<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(hForecast$fitted))),frac=0),yhat=as.numeric(hForecast$fitted))%>%pivot_wider(names_from=ds,values_from=yhat)

        fct2<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(hForecast$x))),frac=0),yhat=as.numeric(hForecast$x))%>%pivot_wider(names_from=ds,values_from=yhat)

        fct3<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(hForecast$mean))),frac=0),yhat=as.numeric(hForecast$mean))%>%pivot_wider(names_from=ds,values_from=yhat)

        fct1=cbind(fct1, fct3)
        fct1$run_type=run_type
        fct2$run_type='actuals'
        fct=rbind.fill(fct1,fct2)

    fct$rmse=forecast::accuracy(hm1)[2]
    fct$mape=forecast::accuracy(hm1)[5]

    fct$tag=thisTag[[1]]

    fct[fct<0] <- 0

    fct$confidence=mean((hForecast$upper-hForecast$mean)/hForecast$mean)

    return(fct)
}

run_arima<-function(i,data, id, periodsToForecast, start_date, run_type){

    thisTag<-data[i,id]

    #Prep time series
    x<-data$data[[i]]%>%arrange(ds)
    x<-x%>%dplyr::rename(y=value)
    x$ds=as.Date(x$ds)
    ts <- ts(x$y, start=start_date, frequency=12)
    ts=na.remove(ts)

    #Fit models
    hm1=auto.arima(ts)
    #hm1 <- hybridModel(y = ts, models = "ae")

        hForecast<-forecast(hm1,h=periodsToForecast)
        ef1<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(hm1$fitted))),frac=0),yhat=as.numeric(hm1$fitted))%>%pivot_wider(names_from=ds,values_from=yhat)
        
        ef2<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(hm1$x))),frac=0),yhat=as.numeric(hm1$x))%>%pivot_wider(names_from=ds,values_from=yhat)
        
        ef3<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(hForecast$mean))),frac=0),yhat=as.numeric(hForecast$mean))%>%pivot_wider(names_from=ds,values_from=yhat)
        ef1=cbind(ef1, ef3)
        ef1$run_type=run_type
        ef2$run_type='actuals'
        ef=rbind.fill(ef1,ef2)
    
    ef$rmse=forecast::accuracy(hm1)[2]
    ef$mape=forecast::accuracy(hm1)[5]

    ef$tag=thisTag[[1]]

    ef[ef<0] <- 0

    ef$confidence=mean((hForecast$upper-hForecast$mean)/hForecast$mean)

  return(ef)
}

run_ets2<-function(i,data, id, periodsToForecast, start_date, run_type){

    thisTag<-data[i,id]

    #Prep time series
    x<-data$data[[i]]%>%arrange(ds)
    x<-x%>%dplyr::rename(y=value)
    x$ds=as.Date(x$ds)
    ts <- ts(x$y, start=start_date, frequency=12)
    ts=na.remove(ts)

    #Fit models
    hm1=ets(ts)
    #hm1 <- hybridModel(y = ts, models = "ae")
        hForecast<-forecast(hm1,h=periodsToForecast)
        ef1<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(hm1$fitted))),frac=0),yhat=as.numeric(hm1$fitted))%>%pivot_wider(names_from=ds,values_from=yhat)
        
        ef2<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(hm1$x))),frac=0),yhat=as.numeric(hm1$x))%>%pivot_wider(names_from=ds,values_from=yhat)
        
        ef3<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(hForecast$mean))),frac=0),yhat=as.numeric(hForecast$mean))%>%pivot_wider(names_from=ds,values_from=yhat)
        ef1=cbind(ef1, ef3)
        ef1$run_type=run_type
        ef2$run_type='actuals'
        ef=rbind.fill(ef1,ef2)
    
    ef$rmse=forecast::accuracy(hm1)[2]
    ef$mape=forecast::accuracy(hm1)[5]

    ef$tag=thisTag[[1]]

    ef[ef<0] <- 0

    ef$confidence=mean((hForecast$upper-hForecast$mean)/hForecast$mean)

  return(ef)
}

run_tbats<-function(i,data, id, periodsToForecast, start_date, run_type){

    thisTag<-data[i,id]

    #Prep time series
    x<-data$data[[i]]%>%arrange(ds)
    x<-x%>%dplyr::rename(y=value)
    x$ds=as.Date(x$ds)
    ts <- ts(x$y, start=start_date, frequency=12)
    ts=na.remove(ts)

    #Fit models
    hm1=tbats(ts)
    #hm1 <- hybridModel(y = ts, models = "at")
        hForecast<-forecast(hm1,h=periodsToForecast)
        ef1<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(hForecast$fitted))),frac=0),yhat=as.numeric(hForecast$fitted))%>%pivot_wider(names_from=ds,values_from=yhat)
        
        ef2<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(hForecast$x))),frac=0),yhat=as.numeric(hForecast$x))%>%pivot_wider(names_from=ds,values_from=yhat)
        
        ef3<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(hForecast$mean))),frac=0),yhat=as.numeric(hForecast$mean))%>%pivot_wider(names_from=ds,values_from=yhat)
        ef1=cbind(ef1, ef3)
        ef1$run_type=run_type
        ef2$run_type='actuals'
        ef=rbind.fill(ef1,ef2)
    
    ef$rmse=forecast::accuracy(hm1)[2]
    ef$mape=forecast::accuracy(hm1)[5]

    ef$tag=thisTag[[1]]

    ef[ef<0] <- 0

    ef$confidence=mean((hForecast$upper-hForecast$mean)/hForecast$mean)

  return(ef)
}

run_stlm<-function(i,data, id, periodsToForecast, start_date, run_type){

    thisTag<-data[i,id]

    #Prep time series
    x<-data$data[[i]]%>%arrange(ds)
    x<-x%>%dplyr::rename(y=value)
    x$ds=as.Date(x$ds)
    ts <- ts(x$y, start=start_date, frequency=12)
    ts=na.remove(ts)

    #Fit models
    hm1=stlm(ts)
    #hm1 <- hybridModel(y = ts, models = "as")
        hForecast<-forecast(hm1,h=periodsToForecast)
        ef1<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(hm1$fitted))),frac=0),yhat=as.numeric(hm1$fitted))%>%pivot_wider(names_from=ds,values_from=yhat)
        
        ef2<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(hm1$x))),frac=0),yhat=as.numeric(hm1$x))%>%pivot_wider(names_from=ds,values_from=yhat)
        
        ef3<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(hForecast$mean))),frac=0),yhat=as.numeric(hForecast$mean))%>%pivot_wider(names_from=ds,values_from=yhat)
        ef1=cbind(ef1, ef3)
        ef1$run_type=run_type
        ef2$run_type='actuals'
        ef=rbind.fill(ef1,ef2)
    
    ef$rmse=forecast::accuracy(hm1)[2]
    ef$mape=forecast::accuracy(hm1)[5]

    ef$tag=thisTag[[1]]

    ef[ef<0] <- 0

    ef$confidence=mean((hForecast$upper-hForecast$mean)/hForecast$mean)

  return(ef)
}

run_thetam<-function(i,data, id, periodsToForecast, start_date, run_type){

    thisTag<-data[i,id]

    #Prep time series
    x<-data$data[[i]]%>%arrange(ds)
    x<-x%>%dplyr::rename(y=value)
    x$ds=as.Date(x$ds)
   ts <- ts(x$y, start=start_date, frequency=12)
   ts=na.remove(ts)

    #Fit models
    hm1=thetam(ts)
    #hm1 <- hybridModel(y = ts, models = "af")
    hForecast <- forecast(hm1, h = periodsToForecast, level=.95) 
    

        fct1<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(hForecast$fitted))),frac=0),yhat=as.numeric(hForecast$fitted))%>%pivot_wider(names_from=ds,values_from=yhat)

        fct2<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(hForecast$x))),frac=0),yhat=as.numeric(hForecast$x))%>%pivot_wider(names_from=ds,values_from=yhat)

        fct3<-data.frame(ds=zoo::as.Date(zoo::yearmon((time(hForecast$mean))),frac=0),yhat=as.numeric(hForecast$mean))%>%pivot_wider(names_from=ds,values_from=yhat)

        fct1=cbind(fct1, fct3)
        fct1$run_type=run_type
        fct2$run_type='actuals'
        fct=rbind.fill(fct1,fct2)

    fct$rmse=forecast::accuracy(hm1)[2]
    fct$mape=forecast::accuracy(hm1)[5]

    fct$tag=thisTag[[1]]

    fct[fct<0] <- 0

    fct$confidence=mean((hForecast$upper-hForecast$mean)/hForecast$mean)

    return(fct)
}

loe_adjustment <- function(data, actuals, loe_date)
{
library(broom)
library(tidyverse)
library(dplyr)

# Read in analogues
inputFile='/rwi/users/achettiath/MI/LOE/analysis/LOE Curves.xlsx'
inputSheet='Data'
loe_curves<-readxl::read_excel(inputFile,sheet=inputSheet)

# Find fit for decay function
data2=loe_curves[which(loe_curves$months_since_loe>=0),]
fit <- nls(scripts ~ SSasymp(months_since_loe, yf, y0, log_alpha), data = data2)

# Get data to curve to
max_date=max(data$ds)

#set parameters for LOE adjustment
decay_after_year=.05
start<-data[which(data$ds==as.Date(loe_date)),]$value
if(length(start)==0)
{
    start<-actuals[which(actuals$ds==as.Date(loe_date)),]$value
}
end<-start*decay_after_year
decay<- -exp(coef(fit)[3])

dates=seq(from=as.Date(loe_date),to=max_date, by = "month") 

x<-c(0:(length(dates)-1))
y<-end + (start-end)*exp(decay*x)

predict<-data.frame(x,y) 
predict$ds<-dates
predict$value=predict$y
predict$x=NULL
predict$y=NULL


for(id in 1:nrow(predict)){
  data$value[data$ds %in% predict$ds[id]] <- predict$value[id]
}


results <- data[order(data$run_type, data$ds),]

# Make it a wide table
results<-pivot_wider(results,names(results)[names(results) %!in% c("ds", "value")],names_from='ds',values_from='value',)

results$run_type=paste0('LOE')

return(results)
}


run_MI <- function(train_end, forecast_start, forecast_months, real_forecast, id_col_no, data, wd)
{

    raw1=data
    startVector<-c(year(min(raw1$ds)),month(min(raw1$ds)))

    #################################################
    # Model Selection
    #################################################
    if(real_forecast==TRUE)
    {
    selected_models=file.info(list.files(paste0(wd, '/selected_models'), full.names = T))
    selected_models=rownames(selected_models)[which.max(selected_models$mtime)]
    selected_models=readRDS(selected_models)
    }

    #################################################
    # Data Prep
    #################################################
    raw1=raw1[which(raw1$ds<=forecast_start),]

    data<-raw1%>%nest(data=c(ds,value))

    #################################################
    # Results Frame
    #################################################
    if(real_forecast==TRUE)
    {
        results=selected_models
    }

    if(real_forecast==FALSE)
    {
        results=data
    }

    #################################################
    # Set up Parallel Processing
    #################################################
    no_cores <- detectCores() - 1  
    cl <- makeCluster(no_cores, type="FORK")  
    registerDoParallel(cl)  

    #################################################
    # Split Data - Path 1
    #################################################
    forecastable_ts = data[which(data$tag %in% results$tag[results$short_history!=1 & results$no_sales!=1]),]

    #################################################
    # Split Data - Path 1 - ETS
    #################################################

    if(real_forecast==FALSE)
    {
        forecastable_ts1=forecastable_ts
        col_names=c(seq(min(raw1$ds), max(raw1$ds), by="month"))
        fct_output1=data.frame(matrix(NA, nrow = 1, ncol = length(col_names)))
        colnames(fct_output1)=c(col_names)
        run_type='ets'
    }



    fct_output1=foreach(i=1:NROW(forecastable_ts1), .combine=rbind, .errorhandling='remove') %dopar% {

        cat('i is',i,'out of',NROW(forecastable_ts1),'\n') #console message
        ets_fct=tryCatch(run_ets(i=i,data=forecastable_ts1, id='tag', periodsToForecast=forecast_months, start_date=startVector, opt_criteria='mae', run_type=run_type),
        error=function(cond) {return(data.frame(matrix(NA, nrow = 1, ncol = 1)))
        })
        fct_output1=rbind.fill(fct_output1,ets_fct)
        return(fct_output1)
    }

    #################################################
    # Split Data - Path 1 - Prophet
    #################################################
    if(real_forecast==FALSE)
    {
        forecastable_ts2=forecastable_ts
        col_names=c(seq(min(raw1$ds), max(raw1$ds), by="month"))
        fct_output2=data.frame(matrix(NA, nrow = 1, ncol = length(col_names)))
        colnames(fct_output2)=c(col_names)
        run_type='prophet'
    }


    fct_output2=foreach(i=1:NROW(forecastable_ts2), .combine=rbind, .errorhandling='remove') %dopar% {
        cat('i is',i,'out of',NROW(forecastable_ts2),'\n') #console message
        prophet_fct=tryCatch(run_prophet(i=i,data=forecastable_ts2, id='tag', periodsToForecast=forecast_months, run_type=run_type),
        error=function(cond) {return(data.frame(matrix(NA, nrow = 1, ncol = 1)))
        })

        fct_output2=rbind.fill(fct_output2,prophet_fct)
            return(fct_output2)

    }



#     for (i in 1:NROW(forecastable_ts2))
# #for (i in 1:10)
# {
#     cat('i is',i,'out of',NROW(forecastable_ts2),'\n') #console message
#     prophet_fct=tryCatch(run_prophet(i=i,data=forecastable_ts2, id='tag', periodsToForecast=forecast_months, run_type=run_type),
#     error=function(cond) {return(data.frame(matrix(NA, nrow = 1, ncol = 1)))
#     })

#     if(i==1) {fct_output2=rbind.fill(fct_output2,prophet_fct)}
#     if (i>1) {fct_output2=rbind.fill(fct_output2, prophet_fct)}

# }


#################################################
# Split Data - Path 1 - Ensemble
#################################################

if(real_forecast==FALSE)
{
    forecastable_ts3=forecastable_ts[which(forecastable_ts$theta_check==1),]
    col_names=c(seq(min(raw1$ds), max(raw1$ds), by="month"))
    fct_output3=data.frame(matrix(NA, nrow = 1, ncol = length(col_names)))
    colnames(fct_output3)=c(col_names)
    run_type='ensemble'
}


fct_output3=foreach(i=1:NROW(forecastable_ts3), .combine=rbind, .errorhandling='remove') %dopar% {
      cat('i is',i,'out of',NROW(forecastable_ts3),'\n') #console message
    ensemble_fct=tryCatch(run_ensemble(i=i,data=forecastable_ts3, id='tag', periodsToForecast=forecast_months, start_date=startVector, run_type=run_type),
    error=function(cond) {return(data.frame(matrix(NA, nrow = 1, ncol = 1)))
    })

    fct_output3=rbind.fill(fct_output3,ensemble_fct)
    return(fct_output3)

}

#################################################
# Split Data - Path 1 - ARIMA
#################################################

if(real_forecast==FALSE)
{
    forecastable_ts4=forecastable_ts
    col_names=c(seq(min(raw1$ds), max(raw1$ds), by="month"))
    fct_output4=data.frame(matrix(NA, nrow = 1, ncol = length(col_names)))
    colnames(fct_output4)=c(col_names)
    run_type='arima'
}

fct_output4=foreach(i=1:NROW(forecastable_ts4), .combine=rbind,  .errorhandling='remove') %dopar% {
      cat('i is',i,'out of',NROW(forecastable_ts4),'\n') #console message
    arima_fct=tryCatch(run_arima(i=i,data=forecastable_ts4, id='tag', periodsToForecast=forecast_months, start_date=startVector, run_type=run_type),
    error=function(cond) {return(data.frame(matrix(NA, nrow = 1, ncol = 1)))
    })

    fct_output4=rbind.fill(fct_output4,arima_fct)
    return(fct_output4)

}

#################################################
# Split Data - Path 1 - ETS2
#################################################

if(real_forecast==FALSE)
{
    forecastable_ts5=forecastable_ts
    col_names=c(seq(min(raw1$ds), max(raw1$ds), by="month"))
    fct_output5=data.frame(matrix(NA, nrow = 1, ncol = length(col_names)))
    colnames(fct_output5)=c(col_names)
    run_type='ets2'
}


fct_output5=foreach(i=1:NROW(forecastable_ts5), .combine=rbind,  .errorhandling='remove') %dopar% {
      cat('i is',i,'out of',NROW(forecastable_ts5),'\n') #console message
    ets2_fct=tryCatch(run_ets2(i=i,data=forecastable_ts5, id='tag', periodsToForecast=forecast_months, start_date=startVector, run_type=run_type),
    error=function(cond) {return(data.frame(matrix(NA, nrow = 1, ncol = 1)))
    })

    fct_output5=rbind.fill(fct_output5,ets2_fct)

        return(fct_output5)

}


#################################################
# Split Data - Path 1 - TBATS
#################################################

if(real_forecast==FALSE)
{
    forecastable_ts6=forecastable_ts
    col_names=c(seq(min(raw1$ds), max(raw1$ds), by="month"))
    fct_output6=data.frame(matrix(NA, nrow = 1, ncol = length(col_names)))
    colnames(fct_output6)=c(col_names)
    run_type='tbats'
}


fct_output6=foreach(i=1:NROW(forecastable_ts6), .combine=rbind,  .errorhandling='remove') %dopar% {
      cat('i is',i,'out of',NROW(forecastable_ts6),'\n') #console message
    tbats_fct=tryCatch(run_tbats(i=i,data=forecastable_ts6, id='tag', periodsToForecast=forecast_months, start_date=startVector, run_type=run_type),
    error=function(cond) {return(data.frame(matrix(NA, nrow = 1, ncol = 1)))
    })

    fct_output6=rbind.fill(fct_output6,tbats_fct)

        return(fct_output6)

}

#################################################
# Split Data - Path 1 - STLM
#################################################
if(real_forecast==FALSE)
{
    forecastable_ts7=forecastable_ts
    col_names=c(seq(min(raw1$ds), max(raw1$ds), by="month"))
    fct_output7=data.frame(matrix(NA, nrow = 1, ncol = length(col_names)))
    colnames(fct_output7)=c(col_names)
    run_type='stlm'
}

fct_output7=foreach(i=1:NROW(forecastable_ts7), .combine=rbind,  .errorhandling='remove') %dopar% {
      cat('i is',i,'out of',NROW(forecastable_ts7),'\n') #console message
    stlm_fct=tryCatch(run_stlm(i=i,data=forecastable_ts7, id='tag', periodsToForecast=forecast_months, start_date=startVector, run_type=run_type),
    error=function(cond) {return(data.frame(matrix(NA, nrow = 1, ncol = 1)))
    })

    fct_output7=rbind.fill(fct_output7,stlm_fct)

        return(fct_output7)

}


# #################################################
# # Split Data - Path 1 - THETA
# #################################################

if(real_forecast==FALSE)
{
    forecastable_ts9=forecastable_ts[which(forecastable_ts$theta_check==1),]
    col_names=c(seq(min(raw1$ds), max(raw1$ds), by="month"))
    fct_output9=data.frame(matrix(NA, nrow = 1, ncol = length(col_names)))
    colnames(fct_output9)=c(col_names)
    run_type='theta'
}


fct_output9=foreach(i=1:NROW(forecastable_ts9), .combine=rbind,  .errorhandling='remove') %dopar% {
      cat('i is',i,'out of',NROW(forecastable_ts9),'\n') #console message
    thetam_fct=tryCatch(run_thetam(i=i,data=forecastable_ts9, id='tag', periodsToForecast=forecast_months, start_date=startVector, run_type=run_type),
    error=function(cond) {return(data.frame(matrix(NA, nrow = 1, ncol = 1)))
    })

    fct_output9=rbind.fill(fct_output9,thetam_fct)

        return(fct_output9)

}


    #################################################
    # Split Data - Path 1 - Clean and Aggregate Data
    #################################################
    fct_output=data.frame(matrix(NA, nrow = 1, ncol = 1))

if (!is.null(fct_output1))
    {
      fct_output1=fct_output1 %>% dplyr::distinct()
      fct_output=rbind.fill(fct_output, fct_output1)
    }
if (!is.null(fct_output2))
    {
      fct_output2=fct_output2 %>% dplyr::distinct()
      fct_output=rbind.fill(fct_output, fct_output2)
    }
if (!is.null(fct_output3))
    {
      fct_output3=fct_output3 %>% dplyr::distinct()
      fct_output=rbind.fill(fct_output, fct_output3)
    }
if (!is.null(fct_output4))
    {
      fct_output4=fct_output4 %>% dplyr::distinct()
      fct_output=rbind.fill(fct_output, fct_output4)
    }
if (!is.null(fct_output5))
    {
      fct_output5=fct_output5 %>% dplyr::distinct()
      fct_output=rbind.fill(fct_output, fct_output5)
    }
if (!is.null(fct_output6))
    {
      fct_output6=fct_output6 %>% dplyr::distinct()
      fct_output=rbind.fill(fct_output, fct_output6)
    }
if (!is.null(fct_output7))
    {
      fct_output7=fct_output7 %>% dplyr::distinct()
      fct_output=rbind.fill(fct_output, fct_output7)
    }
if (!is.null(fct_output9))
    {
      fct_output9=fct_output9 %>% dplyr::distinct()
      fct_output=rbind.fill(fct_output, fct_output9)
    }

  results=merge(results,fct_output, by.x="tag", by.y="tag", all.x=TRUE, no.dups = TRUE)

    stopImplicitCluster()

    return(results)

}