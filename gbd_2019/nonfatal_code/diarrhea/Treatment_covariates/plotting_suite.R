library(ggplot2)
library(dplyr)
library(data.table)

os <- .Platform$OS.type
if (os == "windows") {
  source("FILEPATH")
} else {
  source("FILEPATH")
}

source(fix_path("FILEPATH"))
source(fix_path("FILEPATH"))
source("FILEPATH")
source(fix_path("FILEPATH"))

if(!exists("hierarchy")){
  hierarchy <- get_location_metadata(location_set_id=22,gbd_round_id=7)
}

load_input <- function(model_num){
  model_name <- NULL
  dat <- model_load(model_num,"data")
  #find output folder path
  test_paths <- list.dirs(fix_path("ADDRESS"),recursive=FALSE)
  stage_paths <- c()
  for(i in 1:4){
    stage_paths[length(stage_paths)+1] <- fix_path("ADDRESS")
  }
  flag <- FALSE
  for(path in stage_paths){
    ids <- read.csv("FILEPATH")
    if(model_num %in% ids){
      flag <- TRUE
      model_name <- names(ids)[which(ids==model_num)]
      if(model_name=="ors"){
        full_name <- "diarrhea_ors"
      }
      else if(model_name=="zinc"){
        full_name <- "diarrhea_zinc"
      }
      else{
        full_name <- "lri_antibiotics"
      }
      break
    }
  }
  if(!flag){
    for(path in test_paths){
      ids <- read.csv("FILEPATH")
      if(model_num %in% ids){
        flag <- TRUE
        model_name <- names(ids)[which(ids==model_num)]
      }
    }
  }
  return(list(dat=dat,name=model_name))
}

load_output <- function(model_num){
  model_name <- NULL
  dat <- model_load(model_num,"raked")
  setnames(dat,"gpr_mean","mean")
  #find output folder path
  test_paths <- list.dirs(fix_path("ADDRESS"),recursive=FALSE)
  stage_paths <- c()
  for(i in 1:4){
    stage_paths[length(stage_paths)+1] <- fix_path("FILEPATH")
  }
  flag <- FALSE
  for(path in stage_paths){
    ids <- read.csv("FILEPATH")
    if(model_num %in% ids){
      flag <- TRUE
      model_name <- names(ids)[which(ids==model_num)]
      if(model_name=="ors"){
        full_name <- "diarrhea_ors"
      }
      else if(model_name=="zinc"){
        full_name <- "diarrhea_zinc"
      }
      else{
        full_name <- "lri_antibiotics"
      }
      break
    }
  }
  if(!flag){
    for(path in test_paths){
      ids <- read.csv("FILEPATH")
      if(model_num %in% ids){
        flag <- TRUE
        model_name <- names(ids)[which(ids==model_num)]
      }
    }
  }
  return(list(dat=dat,name=model_name))
}

###Graphing###
#model outputs vs model outputs, to compare the change in estimates between model runs
make_change_scatter <- function(model1_num,model2_num){
  out1_dat0 <- load_output(model1_num)
  out1_dat <- out1_dat0$dat
  setnames(out1_dat,"mean","mean1")
  out2_dat0 <- load_output(model2_num)
  out2_dat <- out2_dat0$dat
  setnames(out2_dat,"mean","mean2")
  m <- merge(out1_dat,out2_dat,by=c("location_id","year_id","age_group_id","sex_id"))
  if("super_region_name" %not in% names(m)){
    mini_h <- hierarchy[,c("location_id","super_region_name")]
    m <- merge(m,mini_h,by="location_id")
  }
  m$diff <- m$mean2-m$mean1

  fun <- function(m){
    g <- ggplot(m,aes(x=mean1,y=mean2,col=super_region_name))+
      geom_point(size=0.1)+
      geom_abline(slope=1,intercept=0)+
      theme_bw()+
      xlab(paste0("Output of Model ",model1_num))+
      ylab(paste0("Output of Model ",model2_num))+
      ggtitle("Comparison of ST-GPR Model Estimates, matched by location/year")
    print(g)
    return(g)
  }
  return(list(m=m,fun=fun))
}

#model inputs vs model outputs, to assess the accuracy of the model
make_quality_scatter <- function(model_num){
  in_dat0 <- load_input(model_num)
  out_dat0 <- load_output(model_num)
  in_dat <- in_dat0$dat
  out_dat <- out_dat0$dat
  m <- merge(in_dat,out_dat,by=c("location_id","year_id","age_group_id","sex_id"))
  if("super_region_name" %not in% names(m)){
    mini_h <- hierarchy[,c("location_id","super_region_name")]
    m <- merge(m,mini_h,by="location_id")
  }

  m <- subset(m,is_outlier==0)

  fun <- function(m){
    g <- ggplot(m,aes(x=data,y=mean,size=variance,col=super_region_name))+
      geom_point()+
      geom_abline(slope=1,intercept=0)+
      theme_bw()+
      xlab("Input Data")+
      ylab("Model Output")+
      ggtitle("ST-GPR Inputs vs Model Estimates, matched by location/year")+
      xlim(0,1)+
      ylim(0,1)
    print(g)
  }
  return(list(m=m,fun=fun))
}

#model outputs time series
make_time_series <- function(model_num,model2_num=NULL,collapsed_path=NULL){
  if(!is.null(model2_num)&!is.null(collapsed_path)){
    return("Can't do both options at once! Pick either model2_num OR collapsed_path.")
  }
  if(is.null(collapsed_path)){
    in_dat0 <- load_input(model_num)
    in_dat <- in_dat0$dat
    out_dat0 <- load_output(model_num)
    out_dat <- out_dat0$dat

    if(!is.null(model2_num)){
      in_dat0_2 <- load_input(model2_num)
      in_dat2 <- in_dat0_2$dat
      out_dat0_2 <- load_output(model2_num)
      out_dat2 <- out_dat0_2$dat
    }

    plots <- list()
    for(loc_id in unique(in_dat$location_id)){
      print(paste0("Generating plot ",which(unique(in_dat$location_id)==loc_id)," of ",length(unique(in_dat$location_id)),"...\n"))
      loc_name <- unlist(hierarchy[which(hierarchy$location_id==loc_id),"location_name"])
      ihme_id <- unlist(hierarchy[which(hierarchy$location_id==loc_id),"ihme_loc_id"])
      out_ts <- arrange(subset(out_dat,location_id==loc_id),by=year_id)
      in_ts <- in_dat[which(in_dat$location_id==loc_id),c("year_id","data","variance","is_outlier")]

      in_ts$se <- sqrt(in_ts$variance)
      in_ts$ub <- in_ts$data + in_ts$se/2
      in_ts$lb <- in_ts$data - in_ts$se/2
      in_ts$is_outlier <- as.factor(in_ts$is_outlier)

      g <- ggplot()+
        geom_line(data=out_ts,aes(x=year_id,y=mean))+
        geom_ribbon(data=out_ts,aes(x=year_id,ymin=gpr_lower,ymax=gpr_upper,fill="blue"))+
        scale_fill_manual(values = alpha(c("blue"), .2),labels=c("GBD 2020"))+
        geom_point(data=in_ts,aes(x=year_id,y=data,col=is_outlier))+
        geom_errorbar(data=in_ts,aes(x=year_id,ymin=lb,ymax=ub,width=0.5,col=is_outlier))+
        scale_color_manual(values = c("black","red"),breaks=c(0,1))+
        theme_bw()+
        xlab("Year")+
        ylab(paste0("Prevalence of ",in_dat0$name))+
        ggtitle("Model Fit vs. Year",subtitle=paste0(loc_name," (",ihme_id,")"))

      if(!is.null(model2_num)){
        out_ts <- arrange(subset(out_dat2,location_id==loc_id),by=year_id)
        in_ts <- in_dat2[which(in_dat2$location_id==loc_id),c("year_id","data","variance","is_outlier")]

        in_ts$se <- sqrt(in_ts$variance)
        in_ts$ub <- in_ts$data + in_ts$se/2
        in_ts$lb <- in_ts$data - in_ts$se/2
        in_ts$is_outlier <- as.factor(in_ts$is_outlier)

        g <- g+
          geom_line(data=out_ts,aes(x=year_id,y=mean),linetype="dashed")+
          geom_ribbon(data=out_ts,aes(x=year_id,ymin=gpr_lower,ymax=gpr_upper,fill="red"))+
          scale_fill_manual(values = alpha(c("blue","red"), .2),labels=c("GBD 2020","GBD 2019"))
      }

      plots[[as.character(loc_id)]] <- g
    }
  } else {
    collapsed_dat <- read.csv(collapsed_path)
    #read collapsed file
    #for each row,
    #match on nid, location_id, sex_id, age_start, year_id
    #merge survey series
    in_dat0 <- load_input(model_num)
    in_dat <- in_dat0$dat
    out_dat0 <- load_output(model_num)
    out_dat <- out_dat0$dat

    match_names <- c("nid","location_id","sex_id","year_id","age_group_id")
    in_dat <- merge(collapsed_dat[,c("nid","location_id","sex_id","year_id","age_group_id","survey_name")],in_dat,by=match_names)

    plots <- list()
    for(loc_id in unique(in_dat$location_id)){
      print(paste0("Generating plot ",which(unique(in_dat$location_id)==loc_id)," of ",length(unique(in_dat$location_id)),"...\n"))
      loc_name <- unlist(hierarchy[which(hierarchy$location_id==loc_id),"location_name"])
      ihme_id <- unlist(hierarchy[which(hierarchy$location_id==loc_id),"ihme_loc_id"])
      out_ts <- arrange(subset(out_dat,location_id==loc_id),by=year_id)
      in_ts <- in_dat[which(in_dat$location_id==loc_id),c("year_id","data","variance","is_outlier","survey_name")]

      in_ts$se <- sqrt(in_ts$variance)
      in_ts$ub <- in_ts$data + in_ts$se/2
      in_ts$lb <- in_ts$data - in_ts$se/2
      in_ts$is_outlier <- as.factor(in_ts$is_outlier)

      g <- ggplot()+
        geom_line(data=out_ts,aes(x=year_id,y=mean))+
        geom_ribbon(data=out_ts,aes(x=year_id,ymin=gpr_lower,ymax=gpr_upper,fill="blue"))+
        scale_fill_manual(values = alpha(c("blue"), .2))+
        geom_point(data=in_ts,aes(x=year_id,y=data,col=survey_name))+
        geom_errorbar(data=in_ts,aes(x=year_id,ymin=lb,ymax=ub,width=0.5,col=survey_name))+
        theme_bw()+
        xlab("Year")+
        ylab(paste0("Prevalence of ",in_dat0$name))+
        ggtitle("Model Fit vs. Year",subtitle=paste0(loc_name," (",ihme_id,")"))+
        theme(legend.position="bottom")
      plots[[as.character(loc_id)]] <- g
    }
  }
  return(plots)
}

#mean distribution
make_mean_dist <- function(model_num,df=NULL){
  dat <- load_input(model_num)
  if(is.null(df)){
    df <- dat$dat
  }
  df <- arrange(df,by=data)
  df$row <- 1:nrow(df)
  df$ub <- df$data + sqrt(df$variance)
  df$lb <- df$data - sqrt(df$variance)
  name <- dat$name

  g <- ggplot(df,aes(x=row,y=data,col=row%%2))+
    geom_point()+
    geom_errorbar(aes(ymin=lb,ymax=ub))+
    theme_bw()+
    ggtitle("Standard Error of New Input Data (sorted by mean value)",subtitle=name)
}

#variance vs sample size
make_var_dist <- function(model_num){
  dat <- load_input(model_num)
  df <- dat$dat
  name <- dat$name
  g <- ggplot(df,aes(x=sample_size,y=sqrt(variance),col=is_outlier))+
    geom_point()+
    scale_x_continuous(trans='log10')+
    theme_bw()+
    xlab("log(sample_size)")+
    ggtitle("Standard Error vs. Sample Size",subtitle=name)
  return(g)
}

save_time_series <- function(root,treatment_name,plots){
  pdf(file=paste0(root,treatment_name,".pdf"))
  for (plot in plots)
  {
    print(plot)
  }
  dev.off()
}

make_violin <- function(model_num,cutoff=5){
  in_dat0 <- load_input(model_num)
  in_dat <- in_dat0$dat
  name <- in_dat0$name
  cutoffs <- list(cutoff/100,1-cutoff/100)
  #load haqi for all locations/years
  haqi <- get_covariate_estimates(location_id="all",covariate_id=1099,year_id="all",decomp_step="iterative")
  #merge haqi and input data
  dat <- merge(in_dat,haqi,by=c("location_id","year_id"))
  #quantiles(HAQI DATA HERE,c(0,0.2,0.4,0.6,0.8,1))
  qs <- quantile(haqi$mean_value,c(0.2,0.4,0.6,0.8,1))
  #find haqi value for each input point, assign quintile (factor("1st","2nd","3rd","4th","5th"))
  dat$quintile <- as.factor(sapply(dat$mean_value,function(x){
    if(x < qs[1]){
      return("1st")
    }
    else if(x < qs[2]){
      return("2nd")
    }
    else if(x < qs[3]){
      return("3rd")
    }
    else if(x < qs[4]){
      return("4th")
    }
    else{
      return("5th")
    }
  }))
  #for each quintile, find 1st, 5th, 95th, 99th percentiles
  dat$is_outlier <- 0
  pqs <- data.frame(qs=qs,ps_lower=NA,ps_upper=NA,quintile=NA)
  for(x in 1:length(levels(dat$quintile))){
    pqs[x,"quintile"] <- levels(dat$quintile)[x]
  }
  for(q in 1:length(levels(dat$quintile))){
    #find percentiles
    ps <- quantile(dat[which(dat$quintile==levels(dat$quintile)[q]),"data"]$data,c(unlist(cutoffs[1]),unlist(cutoffs[2])))
    pqs[q,"ps_lower"] <- ps[1]
    pqs[q,"ps_upper"] <- ps[2]
    pqs$row <- 1:nrow(pqs)
    #set outliers based on percentiles
    dat[which((dat$quintile==levels(dat$quintile)[q])&(dat$data > ps[2]|dat$data < ps[1])),"is_outlier"] <- 1
  }
  g <- ggplot(dat,aes(quintile,data,col=quintile))+
    geom_violin()+
    geom_segment(data=pqs,aes(x=row-0.3,xend=row+0.3,y=ps_upper,yend=ps_upper))+
    geom_segment(data=pqs,aes(x=row-0.3,xend=row+0.3,y=ps_lower,yend=ps_lower))+
    theme_bw()+
    xlab("HAQI Quintile")+
    ylab("Treatment Coverage")+
    ggtitle(paste(name,unlist(cutoffs[1]),"/",unlist(cutoffs[2]),"%"))
  return(g)
}

make_err_dist <- function(model_num){
  inputs <- load_input(model_num)$dat
  outputs <- load_output(model_num)$dat
  combo <- merge(inputs[,c("location_id","year_id","age_group_id","sex_id","data")],outputs[,c("location_id","year_id","age_group_id","sex_id","mean")])
  combo$err <- (combo$mean-combo$data)/combo$data
  g <- ggplot(combo,aes(x=err)) + geom_histogram(binwidth=0.5,fill="red") + theme_bw()# + ylim(0,30)
}

rmse <- function(model_num){
  input <- load_input(model_num)$dat
  output <- load_output(model_num)$dat
  combo <- merge(input[,c("location_id","year_id","age_group_id","sex_id","data")],output[,c("location_id","year_id","age_group_id","sex_id","mean")])
  return(sqrt(sum((combo$data - combo$mean)^2)/nrow(combo)))
}

mean_prct_err <- function(model_num){
  input <- load_input(model_num)$dat
  output <- load_output(model_num)$dat

  mpe <- with(combo,(mean-data)/data)
  return(mpe)
}

time_series_suite <- function(model_num,model2_num=NULL,filename,collapsed_path=NULL){
  root <- fix_path("ADDRESS")
  model_num <- model_num
  treatment_name <- filename
  plots <- make_time_series(model_num,model2_num,collapsed_path)
  save_time_series(root,treatment_name,plots)
}
