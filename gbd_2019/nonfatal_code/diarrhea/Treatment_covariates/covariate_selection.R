os <- .Platform$OS.type
if (os == "windows") {
  source("FILEPATH")
} else {
  source("FILEPATH")
}

library(ggplot2,lib=fix_path("ADDRESS"))
library(gridExtra,lib=fix_path("ADDRESS"))
library(metafor, lib=fix_path("ADDRESS"))
library(boot)
library(lme4)
source(fix_path("ADDRESS"))
source(fix_path("ADDRESS"))


getMaster <- function(dat,cov_obj,cov_name,covariate_id){
  start <- Sys.time()
  if(!exists(cov_obj)){
    eval(parse(text=paste(cov_obj," <<- get_covariate_estimates(location_id=\"all\",covariate_id=covariate_id,year_id =\"all\",decomp_step=\"iterative\",gbd_round_id=6)")))
  }
  eval(parse(text=paste("cov_dat <- ",cov_obj)))
  dat <- merge(dat,cov_dat[,c("location_id","year_id","mean_value")],by=c("location_id","year_id"))
  setnames(dat,"mean_value",cov_name)
  end <- Sys.time()
  print(paste(cov_obj,end-start))
  return(dat)
}


#load input data for ors, zinc, lri
dat_ors <- read.csv(fix_path("FILEPATH"))
dat_zinc <- read.csv(fix_path("FILEPATH"))
dat_lri <- read.csv(fix_path("FILEPATH"))
dat_antibiotics <- read.csv(fix_path("FILEPATH"))
setnames(dat_ors,"val","data",skip_absent=TRUE)
setnames(dat_zinc,"val","data",skip_absent=TRUE)
setnames(dat_antibiotics,"val","data",skip_absent=TRUE)
setnames(dat_lri,"val","data",skip_absent=TRUE)


if(!exists("locs")){
  locs <- get_location_metadata(location_set_id = 22)
}

cov_names <- c("haqi","diarrhea_sev","sdi","LDI_pc","maternal_educ_yrs_pc","he_cap","log_the_pc","ANC1_coverage_prop","DTP3_coverage_prop")
master_names <-c("haqi_master","dSEV_master","sdi_master","ldi_master","mated_master","he_master","log_master","anc_master","dtp_master")
cov_ids <- c(1099,740,881,57,463,1089,1984,7,32)
cov_df <- data.frame(name=cov_names,master_name=master_names,ids=cov_ids)
cov_df$name <- as.character(cov_df$name)
cov_df$master_name <- as.character(cov_df$master_name)

#HAQI: "haqi_master","haqi",1099
dat_ors <- getMaster(dat_ors,"haqi_master","haqi",1099)
dat_antibiotics <- getMaster(dat_antibiotics,"haqi_master","haqi",1099)
dat_zinc <- getMaster(dat_zinc,"haqi_master","haqi",1099)
dat_lri <- getMaster(dat_lri,"haqi_master","haqi",1099)
#Diarrhea SEV: "dSEV_master","diarrhea_sev",740
dat_ors <- getMaster(dat_ors,"dSEV_master","diarrhea_sev",740)
dat_antibiotics <- getMaster(dat_antibiotics,"dSEV_master","diarrhea_sev",740)
dat_zinc <- getMaster(dat_zinc,"dSEV_master","diarrhea_sev",740)
dat_lri <- getMaster(dat_lri,"dSEV_master","diarrhea_sev",740)
#SDI: "sdi_master","sdi",881
dat_ors <- getMaster(dat_ors,"sdi_master","sdi",881)
dat_antibiotics <- getMaster(dat_antibiotics,"sdi_master","sdi",881)
dat_zinc <- getMaster(dat_zinc,"sdi_master","sdi",881)
dat_lri <- getMaster(dat_lri,"sdi_master","sdi",881)
#LDI: "ldi_master","LDI_pc",57
dat_ors <- getMaster(dat_ors,"ldi_master","LDI_pc",57)
dat_antibiotics <- getMaster(dat_antibiotics,"ldi_master","LDI_pc",57)
dat_zinc <- getMaster(dat_zinc,"ldi_master","LDI_pc",57)
dat_lri <- getMaster(dat_lri,"ldi_master","LDI_pc",57)
#Maternal Ed: "mated_master","maternal_educ_yrs_pc",463
dat_ors <- getMaster(dat_ors,"mated_master","maternal_educ_yrs_pc",463)
dat_antibiotics <- getMaster(dat_antibiotics,"mated_master","maternal_educ_yrs_pc",463)
dat_zinc <- getMaster(dat_zinc,"mated_master","maternal_educ_yrs_pc",463)
dat_lri <- getMaster(dat_lri,"mated_master","maternal_educ_yrs_pc",463)
#Health Expenditure: "he_master","he_cap",1089
dat_ors <- getMaster(dat_ors,"he_master","he_cap",1089)
dat_antibiotics <- getMaster(dat_antibiotics,"he_master","he_cap",1089)
dat_zinc <- getMaster(dat_zinc,"he_master","he_cap",1089)
dat_lri <- getMaster(dat_lri,"he_master","he_cap",1089)
#Log Expenditure: "log_master","log_the_pc",1984
dat_ors <- getMaster(dat_ors,"log_master","log_the_pc",1984)
dat_antibiotics <- getMaster(dat_antibiotics,"log_master","log_the_pc",1984)
dat_zinc <- getMaster(dat_zinc,"log_master","log_the_pc",1984)
dat_lri <- getMaster(dat_lri,"log_master","log_the_pc",1984)
#ANC: "anc_master","ANC1_coverage_prop",7
dat_ors <- getMaster(dat_ors,"anc_master","ANC1_coverage_prop",7)
dat_antibiotics <- getMaster(dat_antibiotics,"anc_master","ANC1_coverage_prop",7)
dat_zinc <- getMaster(dat_zinc,"anc_master","ANC1_coverage_prop",7)
dat_lri <- getMaster(dat_lri,"anc_master","ANC1_coverage_prop",7)
#DTP3: "dtp_master","DTP3_coverage_prop",32
dat_ors <- getMaster(dat_ors,"dtp_master","DTP3_coverage_prop",32)
dat_antibiotics <- getMaster(dat_antibiotics,"dtp_master","DTP3_coverage_prop",32)
dat_zinc <- getMaster(dat_zinc,"dtp_master","DTP3_coverage_prop",32)
dat_lri <- getMaster(dat_lri,"dtp_master","DTP3_coverage_prop",32)

dat_ors <- merge(dat_ors,locs[,c("location_id","region_id","super_region_id","super_region_name")],by="location_id")
dat_antibiotics <- merge(dat_antibiotics,locs[,c("location_id","region_id","super_region_id","super_region_name")],by="location_id")
dat_zinc <- merge(dat_zinc,locs[,c("location_id","region_id","super_region_id","super_region_name")],by="location_id")
dat_lri <- merge(dat_lri,locs[,c("location_id","region_id","super_region_id","super_region_name")],by="location_id")

scope_ors <-  formula(data ~ haqi + sdi + LDI_pc + maternal_educ_yrs_pc + he_cap + ANC1_coverage_prop+ diarrhea_sev,data=dat_ors)
scope_zinc <-  formula(data ~ haqi + sdi + LDI_pc + he_cap + ANC1_coverage_prop+ diarrhea_sev,data=dat_zinc)
scope_lri <-  formula(data ~ haqi + sdi + LDI_pc + maternal_educ_yrs_pc + he_cap +  ANC1_coverage_prop,data=dat_lri)
scope_antibiotics <-  formula(data ~ haqi + sdi + LDI_pc + he_cap + ANC1_coverage_prop + maternal_educ_yrs_pc,data=dat_antibiotics)

opt_ors <- invisible(step(glm(data ~ 1, data = dat_ors[which(dat_ors$is_outlier==0),], family=binomial(link="logit")),direction="both",scope=scope_ors))
opt_antibiotics <- invisible(step(glm(data ~ 1, data = dat_antibiotics[which(dat_antibiotics$is_outlier==0),], family=binomial(link="logit")),direction="both",scope=scope_antibiotics))
opt_zinc <- invisible(step(glm(data ~ 1,
                               data = subset(dat_zinc,is_outlier==0),
                               family=binomial(link="logit")),
                           direction="both",
                           scope=scope_zinc))
opt_lri <- invisible(step(glm(data ~ 1, data = dat_lri[which(dat_lri$is_outlier==0),], family=binomial(link="logit")),direction="both",scope=scope_lri))

if("super_region_id.x" %in% names(dat_ors)){
  setnames(dat_ors,"super_region_id.x","super_region_id")
}
if("super_region_id.x" %in% names(dat_antibiotics)){
  setnames(dat_antibiotics,"super_region_id.x","super_region_id")
}
if("super_region_id.x" %in% names(dat_zinc)){
  setnames(dat_zinc,"super_region_id.x","super_region_id")
}
if("super_region_id.x" %in% names(dat_lri)){
  setnames(dat_lri,"super_region_id.x","super_region_id")
}
if("region_id.x" %in% names(dat_ors)){
  setnames(dat_ors,"region_id.x","region_id")
}
if("region_id.x" %in% names(dat_antibiotics)){
  setnames(dat_antibiotics,"region_id.x","region_id")
}
if("region_id.x" %in% names(dat_zinc)){
  setnames(dat_zinc,"region_id.x","region_id")
}
if("region_id.x" %in% names(dat_lri)){
  setnames(dat_lri,"region_id.x","region_id")
}
if("super_region_name.x" %in% names(dat_ors)){
  setnames(dat_ors,"super_region_name.x","super_region_name")
}
if("super_region_name.x" %in% names(dat_antibiotics)){
  setnames(dat_antibiotics,"super_region_name.x","super_region_name")
}
if("super_region_name.x" %in% names(dat_zinc)){
  setnames(dat_zinc,"super_region_name.x","super_region_name")
}
if("super_region_name.x" %in% names(dat_lri)){
  setnames(dat_lri,"super_region_name.x","super_region_name")
}

fit_ors <- glm(data ~ sdi + ANC1_coverage_prop + haqi + (1 | super_region_id) + (1 | region_id) + (1 | location_id), data=dat_ors[which(dat_ors$is_outlier==0),], family=binomial(link="logit"))
fit_antibiotics <- glm(data ~ haqi + (1 | super_region_id) + (1 | region_id) + (1 | location_id), data=dat_antibiotics[which(dat_antibiotics$is_outlier==0),], family=binomial(link="logit"))
fit_zinc <- glm(data ~ sdi + (1 | super_region_id) + (1 | region_id) + (1 | location_id), data=dat_zinc[which(dat_zinc$is_outlier==0),], family=binomial(link="logit"))
fit_lri <- glm(data ~ haqi + (1 | super_region_id) + (1 | region_id) + (1 | location_id), data=dat_lri[which(dat_lri$is_outlier==0),], family=binomial(link="logit"))

pred_ors <- inv.logit(predict(fit_ors, dat_ors[which(dat_ors$is_outlier==0),]))
pred_antibiotics <- inv.logit(predict(fit_antibiotics, dat_antibiotics[which(dat_antibiotics$is_outlier==0),]))
pred_zinc <- inv.logit(predict(fit_zinc, dat_zinc[which(dat_zinc$is_outlier==0),]))
pred_lri <- inv.logit(predict(fit_lri, dat_lri[which(dat_lri$is_outlier==0),]))

out_ors <- data.frame(input=dat_ors$data[which(dat_ors$is_outlier==0)],
                      output=pred_ors,
                      super_region_name=dat_ors$super_region_name[which(dat_ors$is_outlier==0)],
                      variance=dat_ors$variance[which(dat_ors$is_outlier==0)])

out_antibiotics <- data.frame(input=dat_antibiotics$data[which(dat_antibiotics$is_outlier==0)],
                      output=pred_antibiotics,
                      super_region_name=dat_antibiotics$super_region_name[which(dat_antibiotics$is_outlier==0)],
                      variance=dat_antibiotics$variance[which(dat_antibiotics$is_outlier==0)])

out_zinc <- data.frame(input=dat_zinc$data[which(dat_zinc$is_outlier==0)],
                       output=pred_zinc,
                       super_region_name=dat_zinc$super_region_name[which(dat_zinc$is_outlier==0)],
                       variance=dat_zinc$variance[which(dat_zinc$is_outlier==0)])

out_lri <- data.frame(input=dat_lri$data[which(dat_lri$is_outlier==0)],
                      output=pred_lri,
                      super_region_name=dat_lri$super_region_name[which(dat_lri$is_outlier==0)]
                      ,variance=dat_lri$variance[which(dat_lri$is_outlier==0)])

rmse <- function(out){
  return(sqrt(sum((out$input - out$output)^2)))
}

make_quality_scatter2 <- function(dat){
  g <- ggplot(dat,aes(x=input,y=output,size=variance))+
    geom_point()+
    geom_abline(slope=1,intercept=0)+
    theme_bw()+
    xlab("Input Data")+
    ylab("Model Output")+
    ggtitle("ST-GPR Inputs vs Model Estimates, matched by location/year")+
    xlim(0,1)+
    ylim(0,1)
  return(g)
}

g_ors <- make_quality_scatter2(out_ors)
g_antibiotics <- make_quality_scatter2(out_antibiotics)
g_zinc <- make_quality_scatter2(out_zinc)
g_lri <- make_quality_scatter2(out_lri)
