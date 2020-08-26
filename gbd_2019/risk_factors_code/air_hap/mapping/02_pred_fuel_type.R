
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 02/13/2019
# Purpose: Generate new mapping model for different fuel types
#          
# source("FILEPATH.R", echo=T)
#***************************************************************************

#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
} else {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
}


# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","lme4","ggplot2")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

in_date <- "091319"
date <- format(Sys.Date(), "%m%d%y")
#date <- "test"

#------------------DIRECTORIES--------------------------------------------------
home_dir <- file.path("FILEPATH")
in_dataset <- paste0(home_dir,"FILEPATH",in_date,".csv")

# out
dir.create(paste0(home_dir,"FILEPATH",date))
dir.create(paste0(home_dir,"FILEPATH",date))

# central functions
source(file.path(central_lib,"FILEPATH.R"))
locs <- get_location_metadata(35)
source(file.path(central_lib,"FILEPATH.R"))

dt <- fread(in_dataset)
dt[,measure_group:=factor(x=measure_group, levels=c("indoor","female","male","child"))]

#------------------PREDICT PM2.5 EXPOSURE---------------------------------------

# Mixed effects model to predict PM value based on SDI. 
# Effects based on solid/non-solid, measurement_group (indoor, male, child <5, female), and
# whether the measurement was at least 24 hrs. 
# Measurements less thatn 24 hours typically tend to be higher bc they are made during cooking.
# Random effect on study to account for multiple measurements per study. Sample-size weighted.

# set clean!= 1, 0 to .5
dt[clean>0 & clean <1, clean:=.5]

formula <- "log(pm_excess) ~ solid + measure_group + measure_24hr + sdi +(1|S.No)"

mod <- lmer(data=dt, as.formula(formula), weight=sample_size)

summary(mod)

save(mod,file=paste0(home_dir,"FILEPATH",date,".RData"))

pred_dt <- data.table(expand.grid(solid=c(0,1),measure_group=c("indoor","male","female","child"),measure_24hr=c(1),S.No=0,sdi=seq(0.01,0.99,0.01)))
if(date=="test"){pred_dt <- data.table(expand.grid(solid=c(0,1),measure_group=c("indoor","female"),measure_24hr=c(1),S.No=0,sdi=seq(0.01,0.99,0.01)))}

# diagnostic Plot
pred_dt[,pred:=predict(mod,pred_dt,allow.new.levels=T) %>% exp]

if(date=="test"){
  pred_dt[solid==0,fuel:="non_solid"]
}else{
  pred_dt[solid==0,fuel:="clean"]
}

pred_dt[solid==1,fuel:="solid"]


if(date=="test"){
  long <- melt.data.table(dt,id.vars=c("location_name","location_id","region_name","super_region_name","sdi","pm_excess","measure_group","measure_24hr","sample_size"), 
                                         measure.vars=c("non_solid","solid"), variable.name="fuel", value.name="fuel_weight")}else{
  long <- melt.data.table(dt,id.vars=c("location_name","location_id","region_name","super_region_name","sdi","pm_excess","measure_group","measure_24hr","sample_size"), 
                                     measure.vars=c("clean","solid"), variable.name="fuel", value.name="fuel_weight")                                      
                                         }
# scale 1/2 fuel studies based on fuel type

long[fuel_weight==0.5 & fuel=="solid",pm_excess:=exp(log(pm_excess)+.5*fixef(mod)["solid"])]
long[fuel_weight==0.5 & fuel!="solid",pm_excess:=exp(log(pm_excess)-.5*fixef(mod)["solid"])]


pdf(paste0(home_dir,"FILEPATH",date,".pdf"),height=8.5,width=11)

ggplot(data=pred_dt,aes(x=sdi,y=pred,color=measure_group))+geom_line()+facet_wrap(~fuel,nrow=1)+
  geom_point(data=long,aes(x=sdi,y=pm_excess,color=measure_group,alpha=fuel_weight,shape=as.factor(measure_24hr),size=sample_size))+ scale_y_log10(limits=c(0.1,30000))+
  scale_alpha(range=c(0,1)) + scale_shape_manual(values=c(1,19)) + labs(title=formula) 

dev.off()


# Predict out for countries and years -------------------------------------

# prep square for predicting out later
sdi <- get_covariate_estimates(covariate_id=881,year_id=1990:2019,decomp_step="step4",location_id = locs[level>=3,location_id])
sdi <- sdi[,.(location_id,year_id,sdi=mean_value,merge=1)]

out <- data.table(expand.grid(solid=1, measure_group=c("indoor","female"), measure_24hr=1, S.No=0, merge=1))

# used for predicting out
out[,c("measure_groupfemale","measure_groupchild","measure_groupmale"):=0]
out[measure_group=="female",c("measure_groupfemale"):=1]

out <- merge(out, sdi, by="merge", allow.cartesian=T)

# Generate draws from fixed effects in model

  # coefficient matrix
  coeff <- as.data.table(as.list(fixef(mod)))
  coefmat <-matrix(unlist(coeff),ncol=ncol(coeff),byrow=T,dimnames=list(c("coef"),names(coeff)))
  
  # covariance matrix
  vcovmat <- vcov(mod)
  vcovmat <- matrix(vcovmat,ncol=ncol(coeff),byrow=T)
  
  # create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
  betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)
  # transpose coefficient matrix
  betas <- t(betadraws)
  row.names(betas) <- names(coeff)
  # create draws based on fixed and random effects
  
  draw_cols <- paste0("draw_",1:1000)
  out[, (draw_cols) := lapply(1:1000, function(x) {exp(betas["(Intercept)", x] +
      ( betas[names(coeff)[2], x] * get(names(coeff)[2]) ) +
      ( betas[names(coeff)[3], x] * get(names(coeff)[3]) ) +
      ( betas[names(coeff)[4], x] * get(names(coeff)[4]) ) +
      ( betas[names(coeff)[5], x] * get(names(coeff)[5]) ) +
      ( betas[names(coeff)[6], x] * get(names(coeff)[6]) ) +
      ( betas[names(coeff)[7], x] * get(names(coeff)[7]) ))})]
  
out$predict_pm <- predict(object=mod, newdata=out, allow.new.levels=T) %>% exp

out <- out[,c("measure_group","location_id","year_id","sdi","predict_pm",paste0("draw_",1:1000))]
setnames(out,"measure_group","grouping")

# Scale female to male and child based on published studies ---------------

crosswalk <- fread(file.path(home_dir,"FILEPATH","FILEPATH.csv"))
crosswalk <- merge(crosswalk,locs[,.(location_id,location_name=location_ascii_name)],by="location_name",all.x=T)

# Subtract off Ambient
air_pm <- get_covariate_estimates(106,decomp_step = "iterative", location_id=c(unique(crosswalk$location_id)),year_id=c(unique(crosswalk$year_id)))
setnames(air_pm,"mean_value","air_pm")
crosswalk <- merge(crosswalk,air_pm[,.(location_id,year_id,air_pm)],by=c("location_id","year_id"),all.x=T)

# Use published outdoor measurement only for PM2.5 studies
crosswalk[,outdoor:=as.numeric(outdoor)]
crosswalk[Pollutant=="PM2.5",outdoor:=air_pm]

crosswalk <- crosswalk[,.(location_id,year_id,location_name,Pollutant,kitchen_pm,kitchen_n,
                          female_pm,female_n,group,pm,n,outdoor)]

crosswalk[,kitchen_pm:=kitchen_pm-outdoor]
crosswalk[,female_pm:=female_pm-outdoor]
crosswalk[,pm:=pm-outdoor]

# indicate whether or not particulate matter is PM2.5
crosswalk[,other_pm_size:=1]
crosswalk[Pollutant=="PM2.5",other_pm_size:=0]

# weight by total number of measurements
crosswalk[,weight:=female_n+n]

# Calculate ratio
crosswalk[,ratio:=pm/female_pm]

crosswalk_mod <- lm(data=crosswalk, log(ratio) ~ group + 0,weights=weight)
summary(crosswalk_mod)
exp(coefficients(crosswalk_mod))

  # coefficient matrix
  coeff_cw <- as.data.table(as.list(coefficients(crosswalk_mod)))
  coefmat <-matrix(unlist(coeff_cw),ncol=ncol(coeff_cw),byrow=T,dimnames=list(c("coef"),names(coeff_cw)))
  
  # covariance matrix
  vcovmat <- vcov(crosswalk_mod)
  vcovmat <- matrix(vcovmat,ncol=ncol(coeff_cw),byrow=T)
  
  # create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
  betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)
  # transpose coefficient matrix
  betas <- t(betadraws)
  row.names(betas) <- names(coeff_cw)
  
# calculate pm estimates for male and child by scaling draws of female by draws of ratios
  
male <- out[grouping=="female"]
male[,grouping:="male"]
male[,c(draw_cols):=lapply(1:1000, function(x){exp(betas["groupmale", x]) * get(paste0("draw_",x))})]

child <- out[grouping=="female"]
child[,grouping:="child"]
child[,c(draw_cols):=lapply(1:1000, function(x){exp(betas["groupchild", x]) * get(paste0("draw_",x))})]

out <- rbind(out,male)
out <- rbind(out,child)


# Save, summarize, and compare to Step 2-----------------------------------

# save
write.csv(out, file = paste0(home_dir,"FILEPATH",date,"FILEPATH", date,  ".csv"), row.names = F)

# summarize
summary <- copy(out)
summary[, lower := apply(.SD, 1, quantile, c(.025)), .SDcols=draw_cols]
summary[, mean := apply(.SD, 1, mean), .SDcols=draw_cols]
summary[, median := apply(.SD, 1, quantile, c(0.5)), .SDcols=draw_cols]
summary[, upper := apply(.SD, 1, quantile, c(.975)), .SDcols=draw_cols]
summary[, conf_int := upper-lower]
summary[, c(draw_cols) :=NULL]

ggplot(data=summary[year_id %in% c(1990,2005,2019) & location_id<500],aes(x=sdi,y=mean,ymin=lower,ymax=upper,color=grouping,fill=grouping))+geom_point()+geom_ribbon(alpha=.2)+scale_y_log10()

write.csv(summary, file = paste0(home_dir,"FILEPATH",date,"FILEPATH", date,  ".csv"), row.names = F)

# Compare to Step 2

step_2 <- fread(paste0(home_dir,"FILEPATH.csv"))

plot <- merge(step_2,summary,by=c("location_id","year_id"))
plot <- plot[year_id %in% c(1990,2000,2019)]

# scale by ratios
# Step 2 ratios
plot[grouping=="female",ratio:=1.2553055]
plot[grouping=="male",ratio:=0.7149511]
plot[grouping=="child",ratio:=1.0297435]
plot[,predict_pm.x:=predict_pm.x*ratio]

ggplot(plot[grouping!="indoor"],aes(x=predict_pm.x,y=median))+
  geom_point()+geom_abline(slope=1,intercept=0)+
  scale_y_log10()+scale_x_log10()+
  facet_wrap(~grouping)+
  labs(title="Comparison to previous model",x="Step 2",y="Step 3/4")