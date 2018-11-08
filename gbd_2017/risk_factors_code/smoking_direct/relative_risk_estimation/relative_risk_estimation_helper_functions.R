library(data.table)
library(ggplot2)
library(magrittr)
library(readr)
library(openxlsx)
library(gridExtra)
library(ini)

SOURCE FUNCTIONS

read_data <- function(data.path, metric = "none"){
  #' @description Reads in data formatted in the current concise, extraction template for smoking-outcome pairs. For unbounded upper exposure
  #' ranges, multiply the lower by 1.5. If that exceeds 100, cap at 100 (since cig/day, packyr,yrsmk, rarely above that)
  #' @param data.path The filepath of the data
  #' @param metric The exposure metric you want to subset the data out of..if none returns all of the data. Also transforms and catches
  #' any strange column entries.
  #' @return A data.table of the extraction
  message(data.path)
  data <- openxlsx::read.xlsx(data.path, sheet = "Extraction") %>% as.data.table()
  
  setnames(data, c("author-year", "exposure.lower", "exposure.upper","effect.size.type","exposure.definition"),
           c("author_year","exp_lower", "exp_upper","effect_size_measure","exp_def"))
  data[exp_upper >= 99|is.na(exp_upper), exp_upper := exp_lower * 1.5]
  data[exp_upper >100, exp_upper := 100]
  #' Make quitting data upper bound more plausible by setting to 60
  if(metric == "quit"){
    data[exp_upper >= 60, exp_upper := 60]
  }
  data[,exp_mid := (exp_lower + exp_upper) / 2]
  
  data[age.end>99|is.na(age.end),age.end := 99]
  data[,age.mid := (age.start + age.end)/2]
  
  if(metric != "none"){
    data <- data[grepl(metric, tolower(exp_def)),]
    if(metric == "quit"){
      data[,exp_def := paste0(exp_def,"-",reference.category)]
    }
  }
  
  setnames(data, "author_year", "study")
  data[,author_year := paste(study, study.name,sep="-")]
  
  data
}

plot_data <- function(data, xmin=0, xmax=60, ymin=0, ymax=10, color.var = "author_year"){
  #' @description Plots the data Dismod style, with horizontal length being dose range and vertical length being uncertainty
  #' @param color.var How to color the data points
  plot <- ggplot(data, aes(x=exp_mid, y=mean)) +
    geom_point(aes(color=get(color.var)), alpha=0.5, size=0.5) +
    geom_errorbar(aes(ymin=lower, ymax=upper, color=get(color.var),width=0), size=1,alpha=0.3) +
    geom_errorbarh(aes(xmin=exp_lower, xmax=exp_upper, color=get(color.var),height=0), size=1,alpha=0.3) +
    geom_line(y=1, color='red') +
    coord_cartesian(xlim=c(xmin, xmax), ylim=c(ymin, ymax)) +
    ylab("RR") +
    xlab(metric) +
    ggtitle(paste(cause)) +
    theme_bw() #+
  #theme(legend.direction='vertical', legend.key.size= unit(.2, 'cm'), legend.text = element_text(size=10), legend.justification = 'top') +
  #theme(axis.text = element_text(size=20))
  
  plot
}

prep_ode <- function(raw, covariates, smoothing, dose, adjust=F,midpoint=F, sample=5000, sample_interval=20,start.prior = 1, monotonic = "none",outputs, ones.cov = 1, bound.lower=0, sex_effect = F){ 
  #' @description Takes in a dataset and creates a directory to be prepped and ready for dismod ODE. Creates the 5 necessary flat files.
  #' @param raw - The data set for the model
  #' @param covariates - vector of covariates...must include ones and sex...measure is optional (represents type of effect size, RR OR or HR)
  #' @param smoothing - vector of three numerics, higher value means less smoothing
  #' @param dose - The points for desired output for exposure metric
  #' @param adjust - Whether to adjust exposure metrics between 0-100...in smoking case not as necessary (most metric below 100)
  #' @param sample - Number of draws
  #' @param sample_interval - Time between draws
  #' @param former.never - Whether the curve is a former vs. never curve...basically whether to set prior (RR=1 when exposure =0) or not
  #' @param monotonic - Can be "increase" or "decrease" or "none".
  #' @param outputs - The output folder
  #' @param ones.cov - Either 0 or 1. Whether to set x_ones to 0 or 1. 
  #' @param bound.lower - Whether to set a hard floor on the bottom of the RR...for example not letting it dip below 1. 
  #' @return No output, just creates the directory with 5 .csv's needed for Dismod.
  
  templates <- FILEPATH
  
  if (adjust == TRUE){
    adjustment_factor   <- 100/max(dose)
    dose                <- dose*adjustment_factor
    raw[, exp_lower := exp_lower*adjustment_factor]
    raw[, exp_upper := exp_upper*adjustment_factor]
  }
  
  #Pull out list of studies to produce random effects in rate_in
  studies <- unique(raw$author_year)
  
  raw[, super:="none"]
  raw[, region:="none"]
  raw[, subreg:=author_year]
  raw[, age_lower:=exp_lower]
  raw[, age_upper:=exp_upper]
  
  #Use midpoint instead of upper/lower if setting is on
  if (midpoint == T){
    raw[, hold := age_lower + (age_upper - age_lower)/2]
    raw[, `:=`(age_lower = hold, age_upper = hold)]
  }
  
  #Set covariates
  raw[, x_sex:=0]
  raw[, x_ones:=ones.cov]
  
  #' Adjust everything to RR
  if("measure" %in% covariates){
    #'If only have OR, don't adjust
    if(unique(raw[,effect_size_measure])=="OR"){
      raw[,x_measure:=0]
    } else{
      raw[grepl("RR|HR",effect_size_measure), x_measure:=0]
      raw[effect_size_measure=="OR", x_measure:=1]
    }
  }
  
  #' Create sex variable according to whether data is sex-specific. How Dismod encodes sex effect
  if(sex_effect == T){
    is.sex.specific <- length(raw[,unique(sex)]) == 1
    if(!is.sex.specific){
      raw[sex=="Male", x_sex:=0.5]
      raw[sex=="Female", x_sex:=-0.5]
    }
  }
  
  
  #' Set time to a single year 2000. We're not interested in multiple year estimation.
  raw[, time_lower:=2000]
  raw[, time_upper:=2000]
  
  #' RR follows log-normal distribution. RR also most comparable with incidence
  raw[, data_like:= "log_gaussian"]
  raw[, integrand:= "incidence"]
  raw[, meas_value:=mean]
  raw[, meas_stdev:= ifelse(subreg!="Hirayama 1989-NA", (log(upper) - log(meas_value))/1.96, (log(upper) - log(meas_value))/1.64)]
  
  other <- c("nid", "outcome", "location","study.type", "reference.category", "effect_size_measure")
  
  keep <- c(other, "sex", "age_lower", "age_upper", "time_lower", "time_upper", "super","region","subreg","data_like","integrand","meas_value",
            "meas_stdev", names(raw)[names(raw) %like% "x_"])
  
  #' If cause is a CVD, also want to keep the age column 
  if(cause %in% c("ihd", "stroke", "afib_and_flutter", "aortic_aneurism", "peripheral_artery_disease") & metric == "cig"){keep <- c(keep, "this.age")}
  
  raw <- raw[, keep, with=FALSE]
  
  ##### MAKE DATA IN #####
  data_in <- data.table(age_lower = dose[1:length(dose)-1],
                        age_upper = dose[2:length(dose)],
                        time_lower = 2000,
                        time_upper = 2000,
                        super = "none",
                        region = "none",
                        subreg = "none",
                        data_like = "log_gaussian",
                        integrand = "mtall",
                        meas_value = 0.01,
                        meas_stdev = "inf")
  
  for (cov in covariates){
    data_in[, paste0("x_", cov) := 0]
  }
  
  data_in <- rbind(raw, data_in, fill=TRUE)
  data_in[meas_stdev <= 0, meas_stdev:=0.01]
  
  ##### MAKE RATE IN ####
  rate_vars <- c("iota", "chi", "omega", "rho")
  deriv_vars <- c("diota", "dchi", "domega", "drho")
  
  rate_in <- data.table(expand.grid(type=rate_vars, age=dose))
  deriv_df <- data.table(expand.grid(type=deriv_vars, age=dose[1:length(dose)-1]))
  
  rate_in <- rbind(rate_in[order(type)], deriv_df[order(type)])
  
  rate_in[, `:=` (lower = "_inf",
                  upper = "inf",
                  mean = 0,
                  std = "inf")]
  
  #Change derivatives to be (-inf, inf)
  rate_in[!type %in% c("dchi", "diota", "domega", "drho"), `:=`(lower = 0, upper = 0)]
  
  #Make sure iota is bounded from (0, inf) and choose reasonable mean
                             upper = "inf",
                             mean = 1)]
  
  #' If want to set new lower bound for iota
  rate_in[type=="iota",lower:=bound.lower]
  
  #Bind iota at 0 to any value
  rate_in[(type=="iota") & (age==0), `:=`(lower = start.prior,
                                          upper = start.prior,
                                          mean  = start.prior)]
  
  #If want to enforce monotonicity, then set lower and upper bounds to derivative of iota
  if(monotonic=="increase"){
    rate_in[type=="diota", lower := 0]
  } else if(monotonic=="decrease"){
    rate_in[type=="diota", upper := 0]
  }
  
  ##### MAKE EFFECT IN #####
  effect_in <- fread(paste0(templates, "effect_in.csv"), colClasses=c(std="object"))
  
  #Add on random effects
  add_on_studies <- data.table(integrand = "incidence",
                               name = studies,
                               effect = "subreg",
                               lower = -2,
                               upper = 2,
                               mean = 0,
                               std = 1)
  
  add_on_covariates <- data.table(integrand = "incidence",
                                  effect = "xcov",
                                  name = paste0("x_", covariates[!covariates %in% c("sex", "ones")]),
                                  lower = -2,
                                  upper = 2,
                                  mean = 0,
                                  std = "inf")
  
  if (add_on_covariates[, name] == "x_"){
    add_on_covariates <- data.table()
  }
  
  effect_in <- rbind(effect_in, add_on_studies)
  effect_in <- rbind(effect_in, add_on_covariates)
  
  ##### MAKE PLAIN IN #####
  plain_in <- data.table(name = c("p_zero", "xi_omega", "xi_chi", "xi_iota", "xi_rho"), lower = c(0, 0.1, 0.1, smoothing[1], 0.1),
                         upper = c(1, 0.1, 0.1, smoothing[3], 0.1), mean = c(0.1, 0.1, 0.1, smoothing[2], 0.1), std = c("inf","inf","inf","inf","inf"))
  
  ##### MAKE VALUE IN #####
  value_in <- fread(paste0(templates, "value_in.csv"))
  value_in[name == "num_sample",  value := sample]
  value_in[name == "sample_interval", value := sample_interval]
  
  ##### WRITE DATASETS #####
  dir.create(outputs, showWarnings = FALSE, recursive=TRUE)
  
  write_csv(data_in, paste0(outputs, "/data_in.csv"))
  write_csv(rate_in, paste0(outputs, "/rate_in.csv"))
  write_csv(effect_in, paste0(outputs, "/effect_in.csv"))
  write_csv(plain_in, paste0(outputs, "/plain_in.csv"))
  write_csv(value_in, paste0(outputs, "/value_in.csv"))
}

summarize_outputs <- function(outputs, force = F){
  #' @description Takes Dismod ODE output and creates 1000 draws from it, for each output. 
  #' @param outputs - Filepath for where Dismod directory and outputs is located
  #' @param force - Whether to force/truncate draws or leave as is
  #' @output No output, just writes two csv's for the draws, and summarized draws (mean, lower, upper)
  df <- read.csv(paste0(outputs, "/sample_out.csv")) %>% as.data.table
  
  #Only hold onto last 1000 draws and iota's: These are the actual RR draws
  df <- df[(dim(df)[1] - 999):(dim(df)[1])]
  df <- df[, (colnames(df) %like% "iota") & !(colnames(df) %like% "xi"), with=FALSE ]
  
  #Reshape long for easier manipulation, add on draw column, rename columns and remove strings from exposure column
  df <- melt(df)
  names(df) <- c("exposure", "rr")
  
  df[, exposure := gsub("iota_", "", df$exposure)]
  df[, exposure := as.numeric(exposure)]
  
  #Modify exposure to match adjustment factor
  if (adjust==TRUE){
    df[, exposure := exposure/adjustment_factor]
  }
  
  draws <- rep(seq(0, 999), length(unique(df$exposure)))
  df[, draw :=draws]
  
  if(force==T){df[rr < 1, rr:= 1]; df[rr > 10, rr:=10]}
  
  
  df[, `:=`(mean  = mean(.SD$rr),
            lower = quantile(.SD$rr, 0.025),
            upper = quantile(.SD$rr, 0.975)),
     by=c("exposure")]
  
  #' Read in what the sex of the dataset and model is
  df_data <- fread(paste0(outputs, "/data_in.csv"))
  df_data <- df_data[integrand != "mtall",]
  sex <- ifelse(length(df_data[,unique(sex)]) == 1, df_data[,unique(sex)],"Both")
  
  df[,sex:=sex]
  df_compressed <- unique(df[, .(exposure, mean, lower, upper)])
  df_compressed[,sex:=sex]
  
  #' Write outputs
  write_csv(df[, .(exposure, sex, draw, rr)], paste0(outputs, "/rr_draws.csv"))
  write_csv(df_compressed, paste0(outputs, "/rr_summary.csv"))
}

plot_risk <- function(df_data, df_compressed, xmin=0, xmax, ymin=0, ymax, color.line = "sex", color.var="study",show.legend=F){
  #' @description Plots the ODE risk curve, with input data
  #' @param df_data data.table of the data
  #' @param df_compressed data.table of the risk curve, mean lower and upper
  #' @param color.line Variable in the risk curve that you want to color different risk curves by
  #' @param color.var Variable in the data that you want to color different data points by
  #' @return Plot of data and curve
  
  plot <- ggplot(df_compressed, aes(x=exposure, y=mean)) +
    geom_point(aes(shape=get(color.line))) +
    geom_line(aes(linetype=get(color.line))) +
    geom_point(data=df_data, aes(color=get(color.var)), alpha=0.5, size=0.5) +
    geom_errorbar(data=df_data, aes(ymin=lower, ymax=upper, color=get(color.var),width=0), alpha=0.3,size=1) +
    geom_errorbarh(data=df_data, aes(xmin=exposure_lower, xmax=exposure_upper, color=get(color.var),height=0), size=1,alpha=0.3) +
    geom_ribbon(data=df_compressed, aes(ymin=lower, ymax=upper,fill=get(color.line)), alpha=0.2) +
    geom_line(y=1, color='red') +
    coord_cartesian(xlim=c(xmin, xmax), ylim=c(ymin, ymax)) +
    ylab("RR") +
    xlab(metric) +
    ggtitle(paste(cause)) +
    #guides(color=get(color.var)) +
    theme_bw() +
    #guides(col = guide_legend(nrow=50, byrow=TRUE)) +
    theme(legend.direction='vertical', legend.key.size= unit(.2, 'cm'), legend.text = element_text(size=10), legend.justification = 'top') +
    theme(axis.text = element_text(size=20))# + 
  geom_segment(aes(x=xmin, xend=xmax, y=1,yend=1),color="#CC6666")
  plot
}

qsub_ode <- function(job_name, script, slots, memory, arguments=NULL, hold=NULL, cluster_project=NULL, logs.o=NULL, logs.e=NULL,cwd=TRUE) {
  #' @description Submits a job to run dismod. Calls the ODE shell script.
  #' @param job_name Name of the job
  #' @param script The shell script to be used
  #' @param slots Number of slots for job
  #' @param memory 2 * slots GB
  #' @param arguments Any necessary arguments to the script/job
  #' @param cluster_project 
  #' @param logs.o and logs.e Filepath to send log files
  
  command <- "qsub"
  
  ## Required
  ## Job Name
  name <- paste0("-N ", job_name)
  
  ## Slots
  slots <- paste0("-pe multi_slot ", slots)
  memory <- paste0("-l mem_free=", memory, "g")
  
  command <- paste(command, name, slots, memory, sep=" ")
  
  ## Optional
  ## Cluster project
  if (!is.null(cluster_project)) {
    project <- paste0("-P ", cluster_project)
    command <- paste(command, project, sep=" ")
  }
  ## Hold
  if (!is.null(hold)) {
    hold <- paste0("-hold_jid ", hold)
    command <- paste(command, hold, sep=" ")
  }
  ## Logs
  if (!is.null(logs.o) & !is.null(logs.e)) {
    logs <- paste("-o ", logs.o, "-e ", logs.e, sep=" ")
    command <- paste(command, logs, sep=" ")
  }
  ## Current Working Directory
  if (cwd) {
    command <- paste(command, "-cwd", sep=" ")
  }
  
  ## Script
  command <- paste(command, sep=" ")
  
  ## Arguments, the first argument is the script
  if (!is.null(arguments)) {
    args <- paste(gsub(", ", " ", toString(arguments)))
    command <- paste(command, script, args, sep=" ")
  }
  
  ## Submit Job
  system(command)
}

combine_outputs <- function(paths, feature_list = NULL){
  #' @description Rbinds summary and data between outputs to ease manipulation. Number of filepaths must be equal to length of feature_list, or blank
  #' Mostly useful for comparing old and new curves, male to female, age groups, etc.
  #' @param paths - a vector of filepaths containing Dismod ODE outputs. Usually for combining sexes for an exposure metric.
  #' @param feature_list - a string vector that contains the identifiers that separate each curve apart, for manipulation later
  #' @return A list of two elements: The combined data with a new column identifying uniqueness, and combined risk curve table with same new column
  
  if(length(paths) != length(feature_list) & !is.null(feature_list)) stop("length(paths) must equal length(feature_list)")
  if(is.null(feature_list)) feature_list <- rep(NA, length(paths))
  
  x <- cbind(paths, feature_list)
  path.and.feature <- lapply(1:nrow(x), function(i) x[i,])
  df_compressed <- lapply(path.and.feature, function(l) fread(paste0(l[1], "/rr_summary.csv"))[,feature:=l[2]]) %>% rbindlist(fill=T)
  df_data <- lapply(path.and.feature, function(l) fread(paste0(l[1], "/data_in.csv"))[,feature:=l[2]]) %>% rbindlist(fill=T)
  df_draws <- lapply(path.and.feature, function(l) fread(paste0(l[1], "/rr_draws.csv"))[,feature:=l[2]]) %>% rbindlist(fill=T)
  
  #' Specify unique so that in the case of combining two outputs that used same data, not plotting same data twice
  list(unique(df_data), df_compressed, df_draws)
}

calculate_fit <- function(path){
  #' @description Takes in the data and summarized curve created from summarize_outputs() and calculates fits
  #' @output A list of four fit statistics: RMSE, MAD, In-Sample Coverage, and Interval Overlap Coverage.
  df_compressed <- fread(paste0(path, "/rr_summary.csv"))
  df_data <- fread(paste0(path, "/data_in.csv")) %>% format_data
  
  #Calculate RMSE for values lower than 100
  pred <- approx(df_compressed$exposure, df_compressed$mean, df_data$exposure)$y
  df_data[(exposure > 0), pred:=pred]
  
  rmse <- sqrt(mean((df_data$mean - df_data$pred)^2, na.rm=TRUE))
  
  #Calculate in-sample coverage, for points within range
  pred_lower <- approx(df_compressed$exposure, df_compressed$lower, df_data$exposure)$y
  pred_upper <- approx(df_compressed$exposure, df_compressed$upper, df_data$exposure)$y
  
  
  df_data[, `:=`(pred_lower = pred_lower, pred_upper = pred_upper)]
  df_data <- df_data[, is := ifelse((mean >= pred_lower) & (mean <= pred_upper), 1, 0)]
  df_data[, ui := ifelse(upper %between% list(pred_lower, pred_upper) | 
                           lower %between% list(pred_lower, pred_upper)| 
                           (pred_lower %between% list(lower,upper) & (pred_upper %between% list(lower,upper))), 1, 0)]
  
  is_coverage <- sum(df_data$is, na.rm=T)/nrow(df_data[!is.na(pred)])
  ui_coverage <- sum(df_data$ui, na.rm=T)/nrow(df_data[!is.na(pred)])
  #Calculate overlap of uncertainty intervals coverage
  fits <- data.table(path) %>% data.table(rmse, is_coverage, ui_coverage)
  fits
}

print_pdf <- function(plot, save.path){
  pdf(save.path,paper="a4r",width=11,height=8.5)
  print(plot)
  dev.off()
}

cvd_age <- function(data, ages=seq(40,100,10), terminal.age = 100, extrapolate = T){
  #' @description For CVD causes, create data for causes that is more granular in age. Assumes log-linear relation between age and RR. 
  #' Currently assuming effect of age on RR is independent of effect of cig/day on RR.
  #' @terminal.age The age at which the log(RR) should reach 0. Currently finding this age from analysis of CPS-II data used in GBD 2016
  #' @ages The ages at which the RR's should be calculated
  #' @extrapolate Whether when calculating age-specific RR's to go beyond a given data points age range,or just split within data points range
  #' @return Data table with new column for calculated age and new RR's.
  data <- data[rep(1:.N,length(ages))][,this.age := ages,by=.(author_year,exp_lower, exp_upper)]
  data[,sd := (log(upper)-log(mean))/1.96]
  setnames(data, "mean", "old.mean")
  data[, mean := exp(((log(old.mean) - 0)/(age.mid - terminal.age)) * (as.numeric(this.age) - terminal.age))]
  data[this.age >= terminal.age, mean := 1]
  data[, sd := ((sd - 0)/(age.mid - terminal.age)) * (as.numeric(this.age) - terminal.age)]
  data[sd<0, sd:=0.0001]
  data[, `:=`(lower = exp(log(mean) - 1.96*sd),
              upper = exp(log(mean) + 1.96*sd))]
  
  data
}

format_data <- function(df_data){
  #' @description For dismod data visualization purposes, need to back-transform the log-SD calculated into lower
  #' and upper bounds of the original data.
  df_data <- df_data[integrand != "mtall",]
  df_data[, meas_stdev:=as.numeric(meas_stdev)]
  
  setnames(df_data, c("age_lower", "age_upper", "meas_value", "subreg"), c("exposure_lower", "exposure_upper", "mean", "study"))
  
  if (adjust==TRUE){
    df_data[, exposure_lower := exposure_lower/adjustment_factor]
    df_data[, exposure_upper := exposure_upper/adjustment_factor]
  }
  
  df_data[, lower:= log(mean) - 1.96*meas_stdev]
  df_data[, upper:= log(mean) + 1.96*meas_stdev]
  
  df_data[, `:=`(lower = exp(lower),
                 upper = exp(upper))]
  
  df_data[, exposure := exposure_lower + (exposure_upper - exposure_lower)/2]
  df_data
}

expand_draws <- function(cause, metric, df_draws, age_group_ids = c(2:20,30:32,235)){
  #' @description Takes a set of draws and expands them to proper age and sex groups, formatted long. More unique situation for CVD's like
  #' IHD and Stroke, where only certain age groups need to be replicated at lower and upper ends.
  #' @param df_draws Data.table of draws
  #' @param age_group_ids The age group ID's that the draws are meant to be replicated over
  #' @return draws. 
  
  if(cause %in% c("ihd", "stroke", "afib_and_flutter", "aortic_aneurism", "peripheral_artery_disease") & metric == "cig"){
    rr.ages <- data.table(age=rep((seq(40,100,10)),each=2)[1:13],
                          age_start=seq(35,95,5),
                          age_end=seq(39,100,5),
                          age_group_id = c(12:20,30:32,235))
    
    #' Based on map for rr.ages, need to reproduce the draws for each age_group_id that the respective age corresponds to
    all.ages <- df_draws[,unique(age)]
    replicate <- function(a){
      age_group_ids <- rr.ages[age == a, unique(age_group_id)]
      num <- length(age_group_ids)
      b <- df_draws[age == a][rep(1:.N, num)][,age_group_id := rep(age_group_ids, each = .N/num)]
      b
    }
    
    df_draws <- rbindlist(lapply(all.ages, replicate))
    
    #' Replicate the Age group 12 draws for all age groups below 2 to 11
    x <- df_draws[age_group_id==12]
    x[,age_group_id:=NULL]
    x <- x[rep(1:.N,length(2:11))][,age_group_id:=rep(2:11,each=.N/(10))]
    df_draws <- rbind(x, df_draws)
    
    #' Sex ID 
    if(cause %in% c("ihd", "stroke")){
      df_draws <- df_draws[,sex_id:=ifelse(sex=="Male",1,2)]
      df_draws[,sex:=NULL]
    } else {
      df_draws[,sex:=NULL]
      df_draws <- df_draws[rep(1:.N,2)][,sex_id:=c(1,2),by=.(exposure,draw)]
    }
    
    
  } else{
    #' If not IHD/Stroke Cig - If represent only one sex, replicate for both
    if(length(unique(df_draws[,sex]))==1){
      #' If the draws are for both sexes, then need to replicate the draws for both sex and age groups
      df_draws[,sex:=NULL]
      df_draws <- df_draws[rep(1:.N,2)][,sex_id:=c(1,2),by=.(exposure,draw)]
    } else{
      #' If the draws are unique for Male and Female, just convert to sex ID and delete the sex column
      df_draws <- df_draws[,sex_id:=ifelse(sex=="Male",1,2)]
      df_draws[,sex:=NULL]
    }
    
    #' Replicate draws for each age group
    df_draws <- df_draws[rep(1:.N,length(age_group_ids))][,age_group_id:=age_group_ids,by=.(exposure,draw,sex_id)]
    
    #' sex specific causes
    if(cause %in% c("breast_cancer", "cervical_cancer")){df_draws[sex_id==1, rr:=1]}
    if(cause %in% c("prostate_cancer")){df_draws[sex_id==2, rr:=1]}
  }
  df_draws
}

transform_former <- function(raw, new_max = 10, new_min = 1, bound = "keep"){
  #' @description Takes a dataset with former smoking data - years since quitting, and transforms RR data into a new min max form
  #' @param raw   The dataset, with just years since quitting data
  #' @param bound Either [reflect, truncate, keep]. This is how to handle transformed draws above 1 and below 0. 
  #' Reflect multiplies negative draws by -1 and takes draws above 1 as (2 - x). 
  #' Delete draws above 1 or below 0. 
  #' Turn draws above 1 into 1, and draws below 0 into 0. 
  #' Don't touch the draws - allow them to exceed the 0-1 bounds.  
  test <- copy(raw)
  
  #' 1. Create new log(mean) and log SD columns
  test[, `:=`(log_mean = log(mean), 
              log_sd   = ((log(upper) - log(mean)) / 1.96))]
  
  #' 2. Expand row by 1000 (for 1000 draws), and add a new column with 1000 draws by study-exposure range, sampling from log mean and SD - assumes
  #' uncorrelated exposure ranges
  test <- test[rep(1:.N, each=1000)][, `:=`(draw  = 1:.N,
                                            value = rlnorm(1000, log_mean, log_sd)), 
                                     by = .(author_year, exp_lower, exp_upper, sex, outcome)]
  
  #' 3. Perform max-min transformation by DRAW-study-exposure_range, with the new max and min
  test[, max   := max(value), by = .(author_year, draw, sex, outcome)]
  test[, min   := min(value), by = .(author_year, draw, sex, outcome)]
  test[, value := ((value - min) / (max - min) * (new_max - new_min)) + new_min, by = .(author_year, draw, sex, outcome)]
  
  # if (bound == "reflect"){
  #   test[value > 1, value := 1 - (value -1)]
  #   test[value < 0, value := value * -1]
  # } else if (bound == "truncate"){
  #   #test[value > 1, value := 0.99]
  #   test[value < 0, value := .001]
  # } 
  
  #' Collapse max-min draws
  test[, mean_new    := mean(value), by = .(author_year, exp_lower, exp_upper, sex, outcome)]
  test[, lower_new   := quantile(value, 0.025, na.rm = T), by = .(author_year, exp_lower, exp_upper, sex, outcome)]
  test[, upper_new   := quantile(value, 0.975, na.rm = T), by=.(author_year, exp_lower, exp_upper, sex, outcome)]
  
  #' Get rid of unnecessary columns
  cols <- c("value", "draw", "max", "min", "mean", "upper", "lower")
  test[, (cols) := NULL]
  test <- unique(test)
  setnames(test, c("mean_new", "lower_new", "upper_new"), c("mean", "lower", "upper"))
  
  test
}

impute_std <- function(raw, method = "high"){
  #' @description For rows with missing standard deviation from data extraction, try to impute the standard error
  #' @raw dataset
  #' @param method: Decides how to impute the STD, if there is no other way to impute. 
  #' linear: Fits a linear regression for log(std) ~ log(mean). Predicts the missing upper and lower
  #' based on the found relationship. 
  #' high: Finds the 95th percentile of standard errors. This downweights the study, but doesn't take 
  #' into account that certain size RR's have certain size upper lowers. 
  raw[,log_mean := log(mean)]
  data <- copy(raw)
  data[,meas_stdev := (log(upper) - log(lower))/3.92]
  data2 <- data[!is.na(upper) & mean > 1]
  
  if (method == "high"){
    meas <- quantile(data[,meas_stdev], .95, na.rm = T)
    data[is.na(upper), meas_stdev := meas]
    data[is.na(upper), `:=`(upper = exp(log_mean + 1.96 * meas_stdev),
                            lower = exp(log_mean - 1.96 * meas_stdev))]
  } else if (method == "regress") {
    lm.test <- lm(meas_stdev ~ log_mean, data = data2)
    x <- raw[is.na(upper),.(log_mean)]
    meas <- predict(lm.test, x)
    data[is.na(upper), meas_stdev := meas]
    data[is.na(upper), `:=`(upper = exp(log_mean + 1.96 * meas_stdev),
                            lower = exp(log_mean - 1.96 * meas_stdev))]
  }
  
  data
}

choose_knots <- function(raw, num = 6, method = "equal"){
  #' Returns vector with knots chosen depending number specified and method of knot selection.
  #' Either equally spaced, equal data in each region, or 
  #' @param df: Degrees of freedom. Will select knots and place based on the quantiles
  #' @param num: Number of knots. Includes 0 which must be specified
  #' @param method: equal or density. Depends on whether to have equal spaced knots or 
  #' having probability ones.
  if(method == "equal"){
    m <- raw[,max(exp_upper)]
    knots <- seq(0, m, m / (num-1))
  } else if (method == "density"){
    knots <- c(0,quantile(raw[,exp_mid], probs = seq(0,1, 1/(num-2)))) %>% as.numeric %>% unique
  }
  knots
}


