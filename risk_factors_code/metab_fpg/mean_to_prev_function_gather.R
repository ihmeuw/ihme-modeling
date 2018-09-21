## FPG estimate prevalence from mean
rm(list=ls())
if (Sys.info()["sysname"] == "Darwin") j_drive <- filepath
if (Sys.info()["sysname"] == "Linux") j_drive <- filepath
if (Sys.info()["sysname"] == "Windows") j_drive <- filepath

library(data.table)
library(dplyr)
library(rio)
library(mvtnorm)

## regress
ms <- import(filepath)
lm = lm(log_std_dev ~ log_meas_value, data=ms)

# lm = Gls(log_std_dev ~ log_meas_value, data=ms)

## get list of distributions to generate density
source(filepath)
dlist <- c(classA[-9],classB,classM)

ages <- unique(ms$age)
sexes <- c(1,2)

W = data.table()
for (a in ages) {
  for (s in sexes) {
    w <- import(filepath)
    W <- bind_rows(W,w)
  }
}

## fpg mean to prevalence and propogate uncertainty from mean input
fpg_mean_pred_prev_ui <- function(mean,lower,upper,age,sex) {
  a = age
  s = sex
  M = mean
  
  out <- W %>% dplyr::filter(age==a & sex==s)
  
  pred = predict(lm, newdata=data.frame(log_meas_value=log(M),age=a,sex=s),interval="predict")
  pred = exp(pred)
  
  # Draws from the FEs
  N <- 1000
  draws_fe <- rmvnorm(n=N, lm$coefficients, vcov(lm))
  
  sd_mean = (log(upper) - log(lower)) / (2*qnorm(.975))
  
  sim = 1000
  obs = 1
  fpg_draws = exp(data.frame(matrix(rnorm(obs*sim), obs, sim) * sd_mean + log(mean)))
  
  dt <- matrix(0, ncol = 1000, nrow = 1)
  dt <- data.frame(dt,check.names=FALSE)
  
  for (i in 1:1000) {
    dt[,i] = exp((draws_fe[i,"(Intercept)"] + draws_fe[i,"log_meas_value"]*log(fpg_draws[1,i])))[[1]]
  }
  
  SD_PRED = rowMeans(dt,na.rm=T)
  l_SD_PRED = quantile(dt,.025,na.rm=T)[[1]]
  u_SD_PRED = quantile(dt,.975,na.rm=T)[[1]]
  
  oo <- out %>% dplyr::select(-age,-sex,-prev,-obs)
  oo <- as.data.frame(oo)
  ## remove any NA columns
  o <- oo[, colSums(is.na(oo)) != nrow(oo)]
  
  ## predict prev
  x=seq(3,15,length=1000)
  
  fx = 0*x
  
  XMIN <<- 3
  XMAX <<- 15
  
  ## if the fit fails, we need to re-scale the weights so they sum to 1
  Error = 0
  UPSAMPLE = "NA "
  for(z in 1:length(o)){ 
    distn = names(o)[[z]]
    est = try((unlist(dlist[paste0(distn)][[1]]$mv2par(M, (SD_PRED^2)))),silent=T)
    fxj = try((dlist[paste0(distn)][[1]]$dF(x,est)),silent=T)
    N <- try(sum(is.na(fxj)),silent=T) 
    
    if(class(est)=="try-error" | N>500) {
      UPSAMPLE = paste(UPSAMPLE,distn)
      Error = Error + 1
    }
  }
  
  ## upsample
  UPSAMPLE = gsub("NA ","",UPSAMPLE)
  UPSAMPLE = trimws(UPSAMPLE)
  
  if (UPSAMPLE!="") {
    UPS <- unlist(strsplit(UPSAMPLE, split=" "))
    filter <- o
    
    for (u in UPS) {
      filter <- filter %>% dplyr::select(-get(paste0(u)))
    }
    filter = filter/sum(filter)
  }
  
  if (UPSAMPLE=="") {
    filter = o
  }
  
  ## loop through weights and create density vector given mean and sd
  ## mean
  o <- filter
  for(i in 1:length(o)){ 
    distn = names(o)[[i]]
    est = try((unlist(dlist[paste0(distn)][[1]]$mv2par(M, (SD_PRED^2)))))
    fxj = try(dlist[paste0(distn)][[1]]$dF(x,est))
    fx = fx + fxj*o[[i]]
  } 
  fx[is.infinite(fx)] <- 0 
  ## predict prev
  dOUT <- bind_cols(as.data.table(x),as.data.table(fx))
  dOUT <- do.call(data.frame,lapply(dOUT, function(x) replace(x, is.infinite(x),NA)))
  SUM <- sum(dOUT$fx,na.rm=T)
  PROP <- dOUT %>% dplyr::filter(x>=7) %>% dplyr::summarise(sSUM = sum(fx)) %>% dplyr::mutate(prev=sSUM/SUM) %>% dplyr::select(prev)
  PROP <- PROP[[1]]
  
  ## lower
  fx = 0*x
  
  for(i in 1:length(o)){ 
    distn = names(o)[[i]]
    est = (unlist(dlist[paste0(distn)][[1]]$mv2par(M, (l_SD_PRED)^2)))
    fxj = dlist[paste0(distn)][[1]]$dF(x,est) 
    fx = fx + fxj*o[[i]]
  } 
  fx[is.infinite(fx)] <- 0 
  ## predict prev
  dOUT <- bind_cols(as.data.table(x),as.data.table(fx))
  dOUT <- do.call(data.frame,lapply(dOUT, function(x) replace(x, is.infinite(x),NA)))
  SUM <- sum(dOUT$fx,na.rm=T)
  PROP_LOWER <- dOUT %>% dplyr::filter(x>=7) %>% dplyr::summarise(sSUM = sum(fx)) %>% dplyr::mutate(prev=sSUM/SUM) %>% dplyr::select(prev)
  PROP_LOWER <- PROP_LOWER[[1]]
  
  ## upper
  fx = 0*x
  
  for(i in 1:length(o)){ 
    distn = names(o)[[i]]
    est = (unlist(dlist[paste0(distn)][[1]]$mv2par(M, (u_SD_PRED)^2)))
    fxj = dlist[paste0(distn)][[1]]$dF(x,est) 
    fx = fx + fxj*o[[i]]
  } 
  fx[is.infinite(fx)] <- 0 
  ## predict prev
  dOUT <- bind_cols(as.data.table(x),as.data.table(fx))
  dOUT <- do.call(data.frame,lapply(dOUT, function(x) replace(x, is.infinite(x),NA)))
  SUM <- sum(dOUT$fx,na.rm=T)
  PROP_UPPER <- dOUT %>% dplyr::filter(x>=7) %>% dplyr::summarise(sSUM = sum(fx)) %>% dplyr::mutate(prev=sSUM/SUM) %>% dplyr::select(prev)
  PROP_UPPER <- PROP_UPPER[[1]]
  
  return(list(prevalence_mean=PROP,prevalence_lower=PROP_LOWER,prevalence_upper=PROP_UPPER))
}

## read in data
df <- import(filepath)

fpg_mean_pred_prev_ui(mean=5.3,lower=4.735978,upper=5.864022,age=35,sex=2)
fpg_mean_pred_prev_ui(mean=5.55,lower=.846,upper=10.25,age=60,sex=2)

for(j in 1:nrow(df)) {
  
  tryCatch({
    df[j,'prev_mean'] = fpg_mean_pred_prev_ui(mean =  df[j,'new_mean'],
                                              lower = df[j,'new_lower'],
                                              upper = df[j,'new_upper'],
                                              age  =  df[j,'gbd_age'],
                                              sex  =  df[j,'sex_id'])$prevalence_mean
    
    
    df[j,'prev_lower'] = fpg_mean_pred_prev_ui(mean =  df[j,'new_mean'],
                                               lower = df[j,'new_lower'],
                                               upper = df[j,'new_upper'],
                                               age  =  df[j,'gbd_age'],
                                               sex  =  df[j,'sex_id'])$prevalence_lower
    
    df[j,'prev_upper'] = fpg_mean_pred_prev_ui(mean =  df[j,'new_mean'],
                                               lower = df[j,'new_lower'],
                                               upper = df[j,'new_upper'],
                                               age  =  df[j,'gbd_age'],
                                               sex  =  df[j,'sex_id'])$prevalence_upper
    
  }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
  
  m = df[j,'new_mean']
  p = df[j,'prev_mean']
  pl = df[j,'prev_lower']
  pu = df[j,'prev_upper']
  
  print(paste0("Row:",j," Mean = ",m," Prev = ",p, " Lower Prev = ", pl, " Upper Prev = ", pu))
  
}

df_out <- df %>% filter(new_mean>2) 

# fwrite(df_out,(filepath)

# fwrite(df_out,filepath)

ggplot() + geom_point(aes(x=df$new_mean,y=df$prev_mean)) + geom_ribbon(aes(x = df$new_mean,ymin = df$prev_lower,ymax = df$prev_upper),alpha = 0.5) + ggtitle("Mean FPG and PREV CW") + labs(x="mean FPG",y="Predicted prevalence") 

ggplot(df_out) + geom_point(aes(y=prev_mean, x=new_mean)) +
  geom_ribbon(aes(ymin=prev_lower, ymax=prev_upper, x=new_mean, fill = "band"), alpha = 0.3)+
  scale_colour_manual("",values="blue")+
  scale_fill_manual("",values="grey12")

ggplot(df_out, aes(new_mean, prev_mean))+
  geom_point()+
  geom_ribbon(aes(ymin=prev_lower,ymax=prev_upper),alpha=0.2)

# ggsave(filepath)

####################################################################
## Read in base 3-15 incremented fpg file and calculate prevalence
####################################################################
## base loop
base <- import(filepath)
df <- base

for(j in 1:nrow(df)) {
  
  tryCatch({
    mean <- df[j,'mean']
    lower <- df[j,'mean'] - 1.96*df[j,'sd']
    upper <- df[j,'mean'] + 1.96*df[j,'sd']
       
    df[j,'prev_mean'] = fpg_mean_pred_prev_ui(mean =  df[j,'mean'],
                                              lower = lower,
                                              upper = upper,
                                              age  =  df[j,'age'],
                                              sex  =  df[j,'sex'])$prevalence_mean
    
    
    df[j,'prev_lower'] = fpg_mean_pred_prev_ui(mean =  df[j,'mean'],
                                               lower = lower,
                                               upper = upper,
                                               age  =  df[j,'age'],
                                               sex  =  df[j,'sex'])$prevalence_lower
    
    df[j,'prev_upper'] = fpg_mean_pred_prev_ui(mean =  df[j,'mean'],
                                               lower = lower,
                                               upper = upper,
                                               age  =  df[j,'age'],
                                               sex  =  df[j,'sex'])$prevalence_upper
    
  }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
  
  # m = df[j,'new_mean']
  # p = df[j,'prev_mean']
  # pl = df[j,'prev_lower']
  # pu = df[j,'prev_upper']

  # print(paste0("Row:",j," Mean = ",m," Prev = ",p, " Lower Prev = ", pl, " Upper Prev = ", pu))
  
}







