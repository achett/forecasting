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
library(tseries)
library(stats)
library(grid)
library(gridExtra)

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
    pf$rmse=rmse(as.numeric(head(c(metrics[1,]), -1)), as.numeric(head(c(metrics[2,]), -1)))
    pf$mape=mape(as.numeric(head(c(metrics[1,]), -1)), as.numeric(head(c(metrics[2,]), -1)))


    pf$tag=thisTag[[1]]

    pf[pf<0] <- 0

  return(pf)

}

run_MI <- function(train_end, forecast_start, forecast_months, real_forecast, id_col_no, data, wd)
{

    raw1=data
    startVector<-c(year(min(data$ds)),month(min(data$ds)))

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


    if(real_forecast==TRUE)
    {
        forecastable_ts1=forecastable_ts[which(forecastable_ts$tag %in% selected_models$tag[selected_models$short_history!=1 & selected_models$no_sales!=1 
        & selected_models$mywinner=="ets"]),]
        col_names=c(seq(as.Date(forecast_start), length.out=forecast_months, by="month"))
        fct_output1=data.frame(matrix(NA, nrow = 1, ncol = length(col_names)))
        colnames(fct_output1)=c(col_names)
        run_type='real forecast'
    }

    if(real_forecast==FALSE)
    {
        forecastable_ts1=forecastable_ts
        col_names=c(seq(min(raw1$ds), max(raw1$ds), by="month"))
        fct_output1=data.frame(matrix(NA, nrow = 1, ncol = length(col_names)))
        colnames(fct_output1)=c(col_names)
        run_type='ets testing'
    }



    fct_output1=foreach(i=1:NROW(forecastable_ts1), .combine=rbind, .errorhandling='remove') %dopar% {

        cat('i is',i,'out of',NROW(forecastable_ts1),'\n') #console message
        ets_fct=tryCatch(run_ets(i=i,data=forecastable_ts1, id='tag', periodsToForecast=forecast_months, start_date=startVector, opt_criteria='mae', run_type=run_type),
        error=function(cond) {return(data.frame(matrix(NA, nrow = 1, ncol = 1)))
        })
        fct_output1=rbind.fill(fct_output1,ets_fct)
        return(fct_output1)
    }

    fct_output1=fct_output1 %>% dplyr::distinct()

    print("hi")

    #################################################
    # Split Data - Path 1 - Prophet
    #################################################
    if(real_forecast==TRUE)
    {
        forecastable_ts2=forecastable_ts
        col_names=c(seq(as.Date(forecast_start), length.out=forecast_months, by="month"))
        fct_output2=data.frame(matrix(NA, nrow = 1, ncol = length(col_names)))
        colnames(fct_output2)=c(col_names)
        run_type='real forecast'
    }

    if(real_forecast==FALSE)
    {
        forecastable_ts2=forecastable_ts
        col_names=c(seq(min(raw1$ds), max(raw1$ds), by="month"))
        fct_output2=data.frame(matrix(NA, nrow = 1, ncol = length(col_names)))
        colnames(fct_output2)=c(col_names)
        run_type='prophet testing'
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


    fct_output2=fct_output2 %>% dplyr::distinct()

    #################################################
    # Split Data - Path 1 - Aggregate Data
    #################################################
    fct_output=rbind.fill(fct_output1, fct_output2)
    results=merge(results,fct_output, by.x="tag", by.y="tag", all.x=TRUE, no.dups = TRUE)

    write.csv(dplyr::select(results, -data), paste0(wd, '/forecast/deliverable/units', format(Sys.time(), "%Y%m%d_%H%M%S"),'.csv'))

    saveRDS(results, paste0(wd, '/forecast/deliverable/units', format(Sys.time(), "%Y%m%d_%H%M%S"),'.rds'))

    #################
    # Model Selection
    #################

    if(real_forecast==FALSE)
    {
        selected_models=results[which(results$run_type!='actuals'),] %>% dplyr::group_by(tag) %>% slice(which.min(rmse))
        actuals=results[which(results$run_type=='actuals'),] %>% dplyr::group_by(tag) %>% slice(which.min(rmse))  
        winner_actuals=rbind(selected_models,actuals)

    }

    selected_models$mywinner=gsub( " .*$", "", selected_models$run_type)

    write.csv(dplyr::select(selected_models, -data), 
    paste0(wd, '/selected_models/selected_models', format(Sys.time(), "%Y%m%d_%H%M%S_"),'.csv'))

    saveRDS(selected_models,
    paste0(wd, '/selected_models/selected_models', format(Sys.time(), "%Y%m%d_%H%M%S_"),'.rds'))


    return(results)

}