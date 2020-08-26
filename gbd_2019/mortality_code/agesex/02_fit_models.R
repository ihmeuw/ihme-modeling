## Title: Fit age and sex models
## Description: Fits the age and sex models, saves model coefficients and random effects

# set-up ------------------------------------------------------------------

rm(list=ls())

library(argparse)
library(data.table)
library(dplyr)
library(foreign)
library(lme4)
library(splines)
library(ggplot2)

parser <- ArgumentParser()
parser$add_argument('--user', type='character', required=TRUE, help='The user for this run of age-sex')
parser$add_argument('--version_id', type='character', required=TRUE, help='The version for this run of age-sex')
parser$add_argument('--version_5q0_id', type='integer', required=TRUE, help='The 5q0 version for this run of age-sex')
args <- parser$parse_args()

user <- args$user
version_id <- args$version_id
version_5q0_id <- args$version_5q0_id

child_data <- "FILEPATH"
output_dir <- "FILEPATH"

if(dir.exists(output_dir)==F) dir.create(output_dir)
if(dir.exists(paste0(output_dir,'/fit_models'))==F) dir.create(paste0(output_dir,'/fit_models'))
if(dir.exists(paste0(output_dir,'/diagnostics'))==F) dir.create(paste0(output_dir,'/diagnostics'))

input_data <- read.dta(paste0(output_dir,'/input_data_original.dta')) %>% as.data.table

# get locations
locs <- read.csv(paste0(output_dir,'/as_locs_for_stata.csv')) %>% as.data.table
locs <- locs[level_all==1,.(ihme_loc_id,standard)]
fake_regions <- read.csv(paste0(output_dir,'/st_locs.csv')) %>% as.data.table
fake_regions <- fake_regions[keep==1]
setnames(fake_regions,'region_name','gbdregion')
fake_regions[,c('location_id','keep'):=NULL]
locs <- merge(locs,fake_regions,by='ihme_loc_id')

# fit sex model -----------------------------------------------------------

dt <- copy(input_data)
dt <- dt[sex!="" & exclude_sex_mod==0,]
dt <- dcast(dt,ihme_loc_id+region_name+year+source~sex,value.var='q_u5')
dt[,q5_sex_ratio:=male/female]
setnames(dt,c('region_name','both'),c('gbdregion','q5_both'))
dt <- dt[,.(ihme_loc_id, year, q5_both, q5_sex_ratio)]

# exclude data for ratio>1.5 or ratio<0.8
dt <- dt[q5_sex_ratio>0.8 & q5_sex_ratio<1.5 & !is.na(q5_sex_ratio)]

# scale ratio to between 0 and 1 and calculate logit
logit <- function(x) {log(x/(1-x))}
dt[,logit_sex_ratio:=(q5_sex_ratio-0.8)/(1.5-0.8)]
dt[,logit_sex_ratio:=logit(logit_sex_ratio)]

# subset data to standard locations
dt <- merge(dt,locs,by=c('ihme_loc_id'))
dt <- dt[standard==1]

# fit mixed model on standard locations data with spline
sex_model <- lme4::lmer(logit_sex_ratio ~ 1 + ns(q5_both, df=4) + (1|gbdregion), data=dt)

# plot for diagnostics
plot_temp <- copy(dt)
plot_temp$predict <- predict(sex_model, newdata=plot_temp)
gg_fit<- ggplot()+geom_point(data=plot_temp,aes(x=q5_both,y=logit_sex_ratio)) +
  geom_line(data=plot_temp,aes(x=q5_both,y=predict),color='red')+
  facet_wrap('gbdregion') +
  ggtitle('Sex model')
pdf(paste0(output_dir,'/fit_models/sex_model_plots_ns.pdf'),width=11,height=8.5)
plot(gg_fit)
print(plot(sex_model))
dev.off()

# save model object for prediction later
saveRDS(sex_model,paste0(output_dir,'/fit_models/sex_model_object.RDS'))

# fit age model -----------------------------------------------------------

dt <- copy(input_data)
dt <- dt[exclude=='keep']
dt[,log_q_u5:=log(q_u5)]
setnames(dt,'region_name','gbdregion')

# subset to standard locations
dt <- merge(dt,locs,by=c('gbdregion','ihme_loc_id'))
dt <- dt[standard==1]

dt[exclude_enn==0 & prob_enn>0, log_prob_enn:=log(prob_enn)]
dt[exclude_lnn==0 & prob_lnn>0, log_prob_lnn:=log(prob_lnn)]
dt[exclude_pnn==0 & prob_pnn>0, log_prob_pnn:=log(prob_pnn)]
dt[exclude_inf==0 & prob_inf>0, log_prob_inf:=log(prob_inf)]
dt[exclude_ch==0 & prob_ch>0, log_prob_ch:=log(prob_ch)]

dt[,yearmerge:=round(year)]

# fetch covariates from 5q0 process
covs <- read.csv(paste0(child_data,'/data/model_input.csv')) %>% as.data.table
covs <- covs[,.(ihme_loc_id,year,hiv,maternal_edu,ldi)]
covs[,yearmerge:=ceiling(year)]
covs[,year:=NULL]
covs[,ldi:=log(ldi)]
covs[,hiv:=log(hiv+0.000000001)]
covs <- unique(covs)

# merge on covariates and reshape
dt <- merge(dt,covs,by=c('ihme_loc_id','yearmerge'),all.x=T,all.y=F,allow.cartesian=T)
dt <- dcast(dt,gbdregion+ihme_loc_id+year+source+age_type+hiv+maternal_edu+ldi+s_comp~sex,
            value.var = c('log_prob_enn','log_prob_lnn','log_prob_pnn','log_prob_inf','log_prob_ch','log_q_u5'))

# loop through sex and age to fit model
pdf(paste0(output_dir,'/fit_models/age_model_plots.pdf'), height=8.5,width=11)
for(sex in c('male','female')){
  for(age in c('enn','lnn','pnn','inf','ch')){
    
    print(paste0('Running model for ',age,' ',sex))
    
    temp <- copy(dt)
    temp <- temp[!is.na(paste0('log_prob_',age,'_',sex)) & exp(get(paste0('log_q_u5_',sex)))>0,]
    setnames(temp,paste0('log_q_u5_',sex),'log_q5_var')
    
    # define and fit model
    if(age == "pnn" | age == "inf" ) betas <- "1 + hiv"
    if(age == "ch") betas <- "1 + hiv + maternal_edu + s_comp"
    if(age == 'enn' | age == 'lnn') betas <- "1"
    outcome <- paste0('log_prob_',age,'_',sex)
    formula <- paste0(outcome,' ~ ',betas,' + ns(log_q5_var, df=4) + (1|gbdregion)')
    age_model <- lme4::lmer(formula=formula, data=temp)
    
    # plot for diagnostics
    plot_temp <- copy(temp)
    plot_temp$predict <- predict(age_model, newdata=plot_temp)
    gg_fit<- ggplot()+geom_point(data=plot_temp,aes(x=log_q5_var,y=get(outcome))) +
      geom_line(data=plot_temp,aes(x=log_q5_var,y=predict),color='red')+
      facet_wrap('gbdregion') +
      ggtitle(paste0('Age model: ',age,' ',sex)) + ylab('log_prob')
    plot(gg_fit)
    print(plot(age_model))
    
    # save model object for predict later
    saveRDS(age_model,paste0(output_dir,'/fit_models/age_model_object_',age,'_',sex,'.RDS'))
    
  } # end loop on age
} # end loop on sex
dev.off()

# END