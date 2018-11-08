#----HEADER----------------------------------------------------------------------------------------------------------------------
# Project: envir_radon
# Purpose: mixed effects model for radon exposure
#********************************************************************************************************************************

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
}

if (Sys.getenv('SGE_CLUSTER_NAME') == "prod" ) {
  
  
} else {
  
}

# set working directories
home.dir <- "FILEPATH"
setwd(home.dir)

#Packages:
lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("lme4","data.table","magrittr","mvtnorm","lattice","ini")


for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}



# set options
draws.required <- 1000
date <- format(Sys.Date(), "%m%d%y")
#********************************************************************************************************************************

#----FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##

#get location metadata
source("FILEPATH/get_location_metadata.R")
locations <- get_location_metadata(location_set_id=22)
locs <- locations[is_estimate==1]

#get covariates
source("FILEPATH/get_covariate_estimates.R")


#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator
#***********************************************************************************************************************


#----Input data---------------------------------------------------------------------------------------------------------
#pull prepped radon data
radon<-fread("FILEPATH/crosswalked_results.csv")

radon[,new:=0]
radon[is.na(ln_LDI_pc),new:=1]

#drop data with missing variance
radon <- radon[!is.na(variance)]

#save a csv with mean and std_dev for exposure_sd calculation
exp_sd <- copy(radon)
setnames(exp_sd,"data","mean")
exp_sd[,std_dev:=sqrt(variance)]
write.csv(exp_sd[,.(mean,std_dev)],paste0("FILEPATH/data_for_exp_sd_calc_",date,".csv"))

#pull covariates
#average mean temperature over time
mean_temp <- get_covariate_estimates(covariate_id=1199,year_id=2005)
#drop unnecessary columns
mean_temp <- mean_temp[,c("model_version_id",
                          "covariate_id",
                          "covariate_name_short",
                          "location_name",
                          "age_group_id",
                          "age_group_name",
                          "sex_id",
                          "lower_value",
                          "upper_value","year_id"):=NULL]

setnames(mean_temp,"mean_value","cv_temp")


# merge covariate onto radon dt
radon<-merge(radon,mean_temp,by=c("location_id"),all.x=TRUE) 

radon <- merge(radon,locs[,.(location_id,location_name,region_name,super_region_name)],by="location_id",all.x=T)

radon[,log_data:=log(data)]

mod <- lmer(log_data ~ cv_temp + (1 | super_region_id) +(1 | region_id) + (1 | location_id), data=radon)



#datasummaries:
#number of datapoints by region_id
regions<-unique(locs[,.(super_region_id,super_region_name,region_id,region_name)])
data_rep<-merge(regions,radon[!is.na(data),.N,by=c("region_id")],by="region_id",all.x=TRUE)



#----DRAWS--------------------------------------------------------------------------------------------------------------
### prep prediction data
prep <- merge(locs[,.(location_id,region_id,super_region_id,super_region_name,region_name)],mean_temp[,.(location_id,cv_temp)],by="location_id",all.x=TRUE)
prep[,pred1:=exp(predict(mod,newdata=prep,allow.new.levels=T))]


# Add on random effects 
ranef<-as.data.table(ranef(mod,condVar=T))
ranef[,grp:=as.numeric(levels(grp))[grp]]
ranef$term <- NULL
unknown_re <- data.table(mean_re=numeric(),sd_re=numeric())
for(i in 1:length(ranef(mod))){
  re<- names(ranef(mod))[i]
  prep <- merge(prep,ranef[grpvar==re,c("grp","condval","condsd")],by.x=re,by.y="grp",all.x=T)
  setnames(prep,c("condval","condsd"),c(paste0(re,"_re"),paste0(re,"_re_sd")))
  unknown_re <- rbind(unknown_re,data.table(mean_re = round(mean(ranef[grpvar==re,condval]),4),
                              sd_re = round(sqrt(ranef[grpvar==re,mean(condsd^2)]+sd(ranef[grpvar==re,condval])^2),4)))
}

prep[is.na(location_id_re),c("location_id_re","location_id_re_sd"):=c(unknown_re[1,1],unknown_re[1,2])]
prep[is.na(region_id_re),c("region_id_re","region_id_re_sd"):=c(unknown_re[2,1],unknown_re[2,2])]
prep[is.na(super_region_id_re),c("super_region_id_re","super_region_id_re_sd"):=c(unknown_re[3,1],unknown_re[3,2])]

cols <- c("constant", "b_temp")

# coefficient matrix
coeff <- cbind(fixef(mod)[[1]] %>% data.table, coef(mod)[[1]] %>% data.table %>% .[, "(Intercept)" := NULL] %>% .[1, ])
colnames(coeff) <- cols
coefmat <- matrix(unlist(coeff), ncol=length(cols), byrow=TRUE, dimnames=list(c("coef"), c(as.vector(cols))))



#predict out (no draws yet)
prep[,pred:=exp(coefmat[1,1]+cv_temp*coefmat[1,2]+location_id_re+region_id_re+super_region_id_re)]

# covariance matrix
vcovmat <- vcov(mod)
vcovmat <- matrix(vcovmat,ncol=length(cols),byrow=T)

draw_nums <- 1:1000
draw_nums_gbd <- 0:999
lre_cols <- paste0("lre_",draw_nums)
rre_cols <- paste0("rre_",draw_nums)
srre_cols <- paste0("srre_",draw_nums)
prep[,(lre_cols):= transpose(lapply(1:nrow(prep),function(x) rnorm(mean=prep$location_id_re[x],sd=prep$location_id_re_sd[x],n=1000)))]
prep[,(rre_cols):= transpose(lapply(1:nrow(prep),function(x) rnorm(mean=prep$region_id_re[x],sd=prep$region_id_re_sd[x],n=1000)))]
prep[,(srre_cols):= transpose(lapply(1:nrow(prep),function(x) rnorm(mean=prep$super_region_id_re[x],sd=prep$super_region_id_re_sd[x],n=1000)))]

# create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)
# transpose coefficient matrix
betas <- t(betadraws)
# create draws based on fixed and random effects

draw_cols <- paste0("draw_", draw_nums_gbd)
draws_radon <- copy(prep)
invisible(
  draws_radon[, (draw_cols) := lapply(draw_nums, function(x) { exp( betas[1, x] +
                                                                         ( betas[2, x] * cv_temp ) +
                                                                      get(paste0("lre_",x)) + get(paste0("rre_",x)) + get(paste0("srre_",x))
                                                                    ) } )] )
### save results
draws_radon <- draws_radon[, c("location_id","pred", draw_cols),with=F]
write.csv(draws_radon,file=paste0("FILEPATH/draws_",date,".csv"),row.names=F)

#calculate mean and CI
exp <- copy(draws_radon)
exp <- as.data.table(exp)
exp[, exp_lower := apply(.SD, 1, quantile, c(.025)), .SDcols=draw_cols]
exp[, exp_mean := apply(.SD, 1, mean), .SDcols=draw_cols]
exp[, exp_median := apply(.SD, 1, quantile, c(0.5)), .SDcols=draw_cols]
exp[, exp_upper := apply(.SD, 1, quantile, c(.975)), .SDcols=draw_cols]
exp[, conf_int := exp_upper-exp_lower]

exp[,c(draw_cols):=NULL]

ggplot(data=exp,aes(x=pred,y=exp_mean))+geom_point()+geom_abline()

write.csv(exp,file=paste0("FILEPATH/summary_",date,".csv"),row.names=F)


#-------------------------------------------------------DIAGNOSTICS---------------------------------------------------------------------------------------------

old.exp <- fread("FILEPATH/summary.csv")


#prep real datapoints
radon <- radon[,.(location_id,data,standard_error,new)]
radon[, exp_lower := data-1.96*standard_error]
radon[, exp_mean := data]
radon[, exp_upper := data+1.96*standard_error]
radon[, exp_median := NA]
radon[, conf_int := exp_upper-exp_lower]
radon[, gbd_year := runif(nrow(radon),2016,2017)] #for graphing purposes
radon[, type := "data"]
radon <- radon[, .(location_id,exp_lower,exp_mean,exp_median, exp_upper, conf_int, gbd_year, type, new)]


old.exp[,"gbd_year":=2016]
old.exp[,type:="est"]
exp[,"gbd_year":=2017]
exp[,type:="est"]


data <- rbind(old.exp,exp[,-c("pred"),with=F],radon,fill=T)
data<-data[order(type,location_id)]


pdf(file="FILEPATH",onefile=T)

for(loc in unique(data$location_id)){

ifelse(nrow(data[location_id==loc & type=="est"])==2,
       p <- ggplot() +
         geom_ribbon(data=data[location_id==loc & type=="est"],
                     aes(x=gbd_year,ymin=exp_lower,ymax=exp_upper),
                     fill='lightskyblue')+
         geom_line(data=data[location_id==loc & type=="est"],
                   aes(x=gbd_year,y=exp_mean),
                   color='midnightblue'),
       p <- ggplot() + 
         geom_pointrange(data=data[location_id==loc & type=="est"],
                         aes(x=gbd_year,y=exp_mean,ymin=exp_lower,ymax=exp_upper),
                         color='midnightblue'))
         
  
p<-p+
    geom_label(data=data[location_id==loc & type=="est"],
              aes(x=gbd_year,y=exp_mean,label=round(exp_mean,2)),
              color='midnightblue')+
    geom_pointrange(data=data[location_id==loc & type=="data"],
                    aes(x=gbd_year,y=exp_mean,ymin=exp_lower,ymax=exp_upper,color=as.factor(new)))+
    ggtitle(paste("Radon Exposure in Location",loc))+
    scale_x_discrete("Change in model and datapoints",limits=c(2016,2017),labels=c("GBD 16","GBD 17"))+
    ylab("Exposure in Bq/m^3")

print(p)

}

dev.off()

