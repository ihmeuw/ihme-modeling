os <- .Platform$OS.type
if (os == "windows") {
  source("FILEPATH")
} else {
  source("FILEPATH")
}

source(fix_path("FILEPATH"))
source(fix_path("FILEPATH"))

#Load input data
setwd(fix_path("ADDRESS"))
in_dat <- read.csv(fix_path("FILEPATH"))
prctl_cutoff <- c(0.05,0.95)

#Load covariates
covariate_id <- 1099 #HAQI=1099, SDI=881
cov_ests <- get_covariate_estimates(location_id="all",covariate_id=covariate_id,year_id="all",decomp_step="iterative")

#merge covariates with inputs
m <- merge(in_dat,cov_ests[,c("location_id","year_id","age_group_id","sex_id","mean_value")],by=c("location_id","year_id","age_group_id","sex_id"))

#estimate quintiles
quants <- quantile(cov_ests$mean_value,c(0,0.2,0.4,0.6,0.8,1))
m$is_outlier <- NA
m$quintile <- NA
for(q in 1:5){
  #define quintile range
  start <- quants[q]
  end <- quants[q+1]
  
  #find points in that range
  inds <- which(is.na(m$quintile)&m$mean_value<end)
  if(q==5){
    inds <- which(m$mean_value >= start & m$mean_value <= end)
  }
  
  #find 1st/99th percentiles
  prctls <- quantile(m[inds,"data"],prctl_cutoff)
  print(prctls)
  
  #record quantile
  m[inds,"quintile"] <- q
  
  #identify points outside that range, outlier them
  m[inds,"is_outlier"] <- 1
  m[which((m$quintile==q&m$data>prctls[1]|prctls[1]==0)&m$data<prctls[2]),"is_outlier"] <- 0
  m[which(m$nid==0|m$variance<10^-7),"is_outlier"] <- 1
}
m$is_outlier <- factor(m$is_outlier,levels=c(0,1))
m$quintile <- factor(m$quintile,levels=c(0,1,2,3,4,5))
out <- m[which(!(is.na(m$variance)&is.na(m$sample_size))),c(names(m)[1:(length(names(m))-2)],"quintile")]
setnames(out,"data","val",skip_absent=TRUE)
write.csv(out,file=fix_path("FILEPATH"))