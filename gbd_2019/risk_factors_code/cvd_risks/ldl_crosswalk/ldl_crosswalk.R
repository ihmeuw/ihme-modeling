####################
## Purpose: Explore LDL data
########################

if(!exists("rm_ctrl")){
  rm(list=objects())
}
start<-Sys.time()

os <- .Platform$OS.type
if (os=="windows") {
  j <- "FILEPATH"
  h <-"FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}
date <- gsub("-", "_", Sys.Date())

library(data.table)
library(readxl)
library(ggplot2)
library(rstan, lib='FILEPATH')
library(dplyr)
library(reshape)
library(Hmisc)

################### SCRIPTS #########################################
code_root <- 'FILEPATH'

source(paste0(code_root, "get_recent.R"))
source(paste0(code_root, "bind_covariates.R"))
source(paste0(code_root, "get_user_input.R"))
source(paste0(code_root, "utility_functions.R"))
source(paste0(code_root, "check_me_names.R"))
source("FILEPATH/data_tests.R")
source("FILEPATH/get_bundle_version.R")

# Function for pasting together strings so that NAs are skipped 
# Used to concat seqs 
paste_missing <- function(..., sep=" ", collapse=NULL) {
  ret <-
    apply(
      X=cbind(...),
      MARGIN=1,
      FUN=function(x) {
        if (all(is.na(x))) {
          NA_character_
        } else {
          paste(x[!is.na(x)], collapse = sep)
        }
      }
    )
  if (!is.null(collapse)) {
    paste(ret, collapse=collapse)
  } else {
    ret
  }
}

################### PATHS AND ARGS #########################################
if(!exists("me")){
  me <- 'ldl'
  decomp_step <- get_step_from_user()
  bundle_version_id <- get_id_from_user(me, 'bundle_version_id')
}

mes<-c("chl", "ldl", "hdl", "tgl")
step_num <- gsub('step', '', decomp_step)

use_covs<-T

cov_list<-c("Mean BMI", "Liters of alcohol consumed per capita", "Age- and sex-specific SEV for Low omega-3") 

xwalk_data_root <- "FILEPATH"

data_folder<-paste0("FILEPATH", me, "_to_cw/") ## contains combined 

output_path<-paste0("FILEPATH", me, "_to_split/to_split_decomp", step_num, "_", date, ".csv")

# Temporary path for saving pdf file
plot_output <- "FILEPATH/ldl_xwalk.pdf"
review_plot <- "FILEPATH/ldl_xwalk_cov.pdf"

stan_path <- "FILEPATH/ldl_xwalk.stan"
stan_path2 <- "FILEPATH/ldl_xwalk2.stan"


################### GET DATA #########################################
required_cols <- c('ihme_loc_id', 'nid', 'file_path', 'smaller_site_unit', 'site_memo', 'year_id', 'sex_id', 'age_start', 'age_end',
                   'me_group', 'me_name', 'measure', 'diagnostic_criteria', 'mean', 'se', 'sample_size', 'cv_new', 'cvd_cv_urbanicity',
                   'data_type', 'seq', 'underlying_nid', 'sex', 'year_start', 'year_end', 'variance', 'location_id')

# Read in data 
df <- get_bundle_version(bundle_version_id)
df <- as.data.table(df)

# Get age start and ends from original data instead of from bundle 
# transformation which uses age group ID to assign age start/end values
df[,age_start:=age_start_orig]
df[,age_end:=age_end_orig]

# Create sex ID column
df[sex=='Male', sex_id := 1]
df[sex=='Female', sex_id := 2]
df[sex=='Both', sex_id := 3]

# Update col names
setnames(df, c('val', 'standard_error'), c('mean', 'se'))

df <- subset(df, select=required_cols)

# check that MEs in the dataset are named what we expect and correct them if they aren't
df <- check_me_names(df)

## drop prev for now
df <- df[measure!='prevalence']

cols<-setdiff(names(df), c("mean", "me_name", "se"))

df$new_sample_size <- as.numeric(df$new_sample_size)
df$ss_diff <- as.numeric(df$ss_diff)

## fix sample sizes for microdata
df[data_type==1, new_sample_size:=round(mean(sample_size)), by=.(nid, ihme_loc_id, year_id, sex_id, age_start, age_end)]
df[data_type==1, ss_diff:=sample_size-new_sample_size]
df[data_type==1, sample_size:=new_sample_size]

## reshape lit data

cols_without_seq <- cols[cols != 'seq']

## drop true duplicates
true_duplicate_indices <- duplicated(df[,c(cols_without_seq, 'me_name', 'mean', 'sample_size'), with=FALSE])
df <- df[!true_duplicate_indices]
message(sprintf('Dropped %d true duplicates', sum(true_duplicate_indices)))

## drop duplicates if necessary, temporary fix
duplicate_indices <- duplicated(df[,c(cols_without_seq, 'me_name'), with=FALSE])
num_duplicates <- sum(duplicate_indices)
if (num_duplicates > 0) {
  duplicates_warning <- paste0('There are duplicates in your data, ',
                               'i.e. different values for the same ',
                               'source, age, sex, year, location, ME, and measure.\n',
                               'They are getting dropped so dcast will work, ',
                               'but look into why this is happening. ',
                               'Press [enter] to continue: ')
  invisible(readline(prompt=duplicates_warning))
  df <- df[!duplicate_indices]
  message(sprintf('Dropped %d duplicates from %d rows', 
                  num_duplicates, nrow(df)))
}

df_without_seqs <- df[,!'seq']

## reshape from long to wide where columns are MEs and values are seqsfor a given data point
cast_formula <- paste0(paste0(cols_without_seq, collapse="+"), "~me_name")
df.t.seqs<-dcast.data.table(df, formula(cast_formula), 
                            value.var=c("seq"), fill=NA)
## Prepend seq col names with 'seq_'
colnames(df.t.seqs)[length(df.t.seqs) - (3:0)] <- paste0('seq_', colnames(df.t.seqs)[length(df.t.seqs) - (3:0)])
## Create parent_seqs col which either gets ldl seq or concats other seqs
df.t.seqs$parent_seqs <- ifelse(!is.na(df.t.seqs$seq_ldl), df.t.seqs$seq_ldl, 
                                paste_missing(df.t.seqs$seq_chl, df.t.seqs$seq_hdl, df.t.seqs$seq_ldl, df.t.seqs$seq_tgl, sep=','))
## Get first seq in parent_seq column to use as crosswalk_parent_seq for uploader
df.t.seqs$seq <- gsub(",.*$", "", df.t.seqs$parent_seqs)

## Reshape so ME values from the same data source for a given age, sex, year, and location are in the same row but spread across columns
df.t <- dcast.data.table(df_without_seqs, formula(paste0(paste0(cols_without_seq, collapse="+"), "~me_name")), value.var=c('mean','se'))

seq_info <- df.t.seqs[,c('seq', 'parent_seqs')]
df.t <- cbind(df.t, seq_info)

if(is.integer(df.t$mean_chl)){stop("Aggregation error, some rows were duplicated")}

setnames(df.t, c("mean_chl", "mean_ldl", "mean_hdl", "mean_tgl"), c("chl", "ldl", "hdl", "tgl"))

## compute ldl from friedewald
df.t[!is.na(chl) & !is.na(hdl) & !is.na(tgl) & is.na(ldl), freid:="Calculated w/ Freidewald"]
df.t[is.na(freid) & !is.na(ldl), freid:="Directly reported"]
df.t[!is.na(chl) & !is.na(hdl) & !is.na(tgl) & is.na(ldl), ldl:=chl-(hdl+tgl/2.2)]

## drop calcd w/ friedwald from nhanes 1991
df.t<-df.t[nid!=NID | freid!="Calculated w/ Freidewald", ]

#df.t <- df.t[age_end>20]
## run simple model of chl vs ldl to get intercept
ldl_mod <- lm(formula=ldl~1+chl, data=df.t[chl<9 & chl>1.5 & sample_size>10])
#ldl_mod<-lm(formula=chl~1, data=df.t)
intercept <- coef(ldl_mod)[["(Intercept)"]]
slope <- coef(ldl_mod)[["chl"]]
correl <- cor(df.t$chl, df.t$ldl, use="complete.obs")

################### PLOTS #########################################
pdf(file=plot_output, width=11.5)
for(me.t in setdiff(mes, "chl")){
  ## histogram of me
  p <- ggplot(data=df.t)+
        geom_histogram(aes(x=get(me.t)), color="black", fill="white")+
        ggtitle(paste0("Distribution of ", me.t))+
        xlab(me.t)+
        theme_classic()+
        theme(text=element_text(size=20))
  print(p)
  
  
  
  ## plot ldl vs chl data
  if(me.t=="ldl"){
    p <- ggplot(data=df.t[sample_size>10])+
          geom_point(aes(x=chl, y=get(me.t), color=factor(freid)), size=2)+
          geom_rug(data=df.t[is.na(get(me.t)) & chl<9 & chl>1.5], aes(x=chl), sides="b")+
          #geom_rug(data=df.t[is.na(chl)], aes(y=ldl), sides="l")+
          geom_abline(aes(intercept=intercept, slope=slope))+
          #geom_abline(aes(intercept=intercept, slope=1))+
          ggtitle(paste0("Total cholesterol vs ", toupper(me.t)))+
          scale_color_discrete(name=NULL)+
          ylab(toupper(me.t))+
          xlab("Total cholesterol")+
          theme_minimal()+
          theme(text=element_text(size=20))
    print(p)
  } else {
    p <- ggplot(data=df.t)+
      geom_point(aes(x=chl, y=get(me.t)), size=2)+
      geom_rug(data=df.t[is.na(get(me.t))], aes(x=chl), sides="b")+
      geom_rug(data=df.t[is.na(chl)], aes(y=get(me.t)), sides="l")+
      ggtitle(paste0("Total cholesterol vs ", toupper(me.t)))+
      ylab(toupper(me.t))+
      xlab("Total cholesterol")+
      theme_minimal()+
      theme(text=element_text(size=20))
    print(p)
  }
}
dev.off()

################### XWALK #########################################
if(use_covs==F){
  mod1 <- stan_model(stan_path)
} else {
  mod2 <- stan_model(stan_path2)
}

## this tells what data is available for each row.. some combo of chl, hdl, and tgl. Used for estimiating different alphas in xwalk
df.t[, alpha_type:="a_"]
for(me.t in setdiff(mes, "ldl"))
  {
    df.t[!is.na(get(me.t)), alpha_type:=paste0(alpha_type, "_", me.t)]
  }

################### GET COVARIATES #########################################
## get covariates, some may be missing because of missingness is covariate database
data_and_names <- bind_covariates(df.t, cov_list=cov_list)
df.t <- data_and_names[[1]] ##sy: this is the data w/ bound covariate estimates
cov_list <- data_and_names[[2]] ##sy: these are the covariate_name_shorts.. stupid that we need to 

df.mod <- copy(df.t)

for(cov in cov_list){
  df.mod <- df.mod[!is.na(get(cov))]
}

df.mod <- df.mod[!alpha_type %in% c("a__tgl", "a__chl_tgl"),  ]

df.mod[, alpha_type:=as.factor(alpha_type)]


################### SETUP DATA #########################################
offset <- 0.001 ## add this to zeros 
ldl <- df.mod[!is.na(ldl)]
no_ldl <- df.mod[is.na(ldl)]


## impute missing ldl SEs, this should be a small number
high_se <- quantile(ldl$se_ldl, probs=0.99, na.rm=T)
message("Imputing ", nrow(ldl[is.na(se_ldl)]), " LDL SEs as ", high_se)
ldl[is.na(se_ldl) | se_ldl==0, se_ldl:=high_se]

## set any missing lipids to 0; this acts as an indicator variable
for(me.t in setdiff(mes, "ldl")){
  ldl[get(me.t)==0, as.character(me.t):=offset]
  ldl[is.na(get(me.t)), (me.t):=0]
  ldl[is.na(get(paste0("se_", me.t))), (paste0("se_", me.t)):=offset^5]
  ldl[get(paste0("se_", me.t))==0, (paste0("se_", me.t)):=high_se]
  
  no_ldl[get(me.t)==0, as.character(me.t):=offset]
  no_ldl[is.na(get(me.t)), (me.t):=0]
  no_ldl[is.na(get(paste0("se_", me.t))), (paste0("se_", me.t)):=offset^5]
  no_ldl[get(paste0("se_", me.t))==0, (paste0("se_", me.t)):=high_se]
  
}

## create covariate matrix
form <- paste0("~0+", paste0(cov_list, collapse="+"))
X <- model.matrix(as.formula(form), ldl)
X <- scale(X)

pred_X <- model.matrix(as.formula(form), no_ldl)
pred_X <- scale(pred_X)

xwalk_data <- list(
  ## input training data
  N=nrow(ldl), ldl=ldl$ldl, chl=ldl$chl, hdl=ldl$hdl, tgl=ldl$tgl,
  ldl_se=ldl$se_ldl, chl_se=ldl$se_chl, hdl_se=ldl$se_hdl, tgl_se=ldl$se_tgl, 
  n_lipids=length(unique(ldl$alpha_type)), lipids=as.numeric(ldl$alpha_type),
  K=length(cov_list), X=X,
  
  ## input prediction data
  pred_N=nrow(no_ldl), pred_lipids=as.numeric(no_ldl$alpha_type),
  pred_chl=no_ldl$chl, pred_hdl=no_ldl$hdl, pred_tgl=no_ldl$tgl,
  pred_chl_se=no_ldl$se_chl, pred_hdl_se=no_ldl$se_hdl, pred_tgl_se=no_ldl$se_tgl,
  pred_X=pred_X
)

################### MODEL #########################################

if(use_covs==T){
  fit <- sampling(mod2, data=xwalk_data, pars=c("linpred_i", paste0(mes, "_est"), paste0(mes, "_pred_est")), include=F, chains=2, iter=1000)
  
  betas <- as.data.table(summary(fit, pars="beta")$summary)
  betas <- cbind(cov_name=cov_list, betas)
} else {
  fit <- sampling(mod1, data=xwalk_data, chains=2, iter=1000)
}

alphas <- as.data.table(summary(fit, pars="alpha")$summary)
alphas <- alphas[, .(mean, sd)]
alphas <- cbind(alpha_type=levels(ldl$alpha_type), alphas)

write.csv(alphas, file="FILEPATH/ldl_xwalk_alphas.csv", row.names = F)

lip_betas <- as.data.table(summary(fit, pars="lip_beta")$summary)
lip_betas <- lip_betas[, .(mean, sd)]
lip_betas <- cbind(lipid=c("TC", "HDL", "TGL"), lip_betas)
write.csv(lip_betas, file="FILEPATH/ldl_xwalk_lipid_betas.csv", row.names = F)

ldl_hats <- as.data.table(summary(fit, pars="ldl_hat")$summary)
ldl_full <- cbind(ldl, ldl_hats[, .(mean, sd)])

ldl_preds <- as.data.table(summary(fit, pars="pred_ldl")$summary)
preds_full <- cbind(no_ldl, ldl_preds[, .(mean, sd)])


################### MODEL ALL REGRESSIONS SEPARATELY #########################################
pred_list <- list()
for(alpha in unique(ldl$alpha_type)){
  if(alpha!="a_"){
    ## get lipids
    lipids <- tstrsplit(alpha, "a__")[[2]]
    lipids <- unlist(tstrsplit(lipids, "_"))
    for(lip in lipids){
      ldl.sub <- ldl[get(lip)>0]
    }
    
    ## run regression
    form <- paste0("ldl~", paste0(lipids, collapse="+"))
    temp_mod <- lm(formula=as.formula(form), data=ldl.sub)
    
    preds <- predict(temp_mod)
    preds <- data.table(preds, alpha, split_id=as.integer(ldl.sub$split_id))
    pred_list[[length(pred_list)+1]]<-preds
  }
}
pred_list <- rbindlist(pred_list)

################### COMPARE RESULTS #########################################
compare <- merge(ldl_full, pred_list, by="split_id", all.y=T)
pdf(file="FILEPATH/ldl_xwalk_compare.pdf")
for(a in unique(ldl$alpha_type)){
  compare.l <- compare[alpha==a & alpha_type==a]
  compare.l <- compare[alpha==a]
   
  ## scatter predictions
  p <- ggplot(data=compare.l, aes(x=preds, y=mean))+
        geom_point()+
        geom_abline(aes(slope=1, intercept=0))+
        xlab("Separate regressions")+
        ylab("One regression")+
        ggtitle(paste0("Prediction comparison for ", a))+
        theme_classic()
  print(p)
  
  ## scatter error
  compare.l[, `:=` (mean_err=mean-ldl, preds_err=preds-ldl)]
  compare.l.cast<-melt(compare.l[, .(split_id, mean_err, preds_err)], id.vars = "split_id") 
  
  p <- ggplot(data=compare.l.cast, aes(x=variable, y=value))+
        geom_boxplot()+
        #geom_abline(aes(slope=1, intercept=0))+
        # xlab("Separate regressions")+
        # ylab("One regression")+
        #facet_wrap(~variable)+
        ggtitle(paste0("Error comparison for ", a))+
        theme_classic()
  print(p)
}
dev.off()


################### PLOT RESULTS #########################################
pdf(file="FILEPATH/ldl_xwalk_results.pdf")
## show model fit
p <- ggplot(data=ldl_full)+
      geom_point(aes(x=ldl, y=mean, color=alpha_type))+
      geom_abline(slope=1, intercept=0)+
      facet_wrap(~alpha_type)+
      theme_classic()+
      xlab("Observed LDL")+
      ylab("Predicted LDL")+
      ggtitle("Single regression")+
      theme(panel.border=element_rect(fill=NA))
print(p)

## plot predictions
p <- ggplot(data=preds_full)+
      geom_histogram(aes(x=mean))+
      facet_wrap(~alpha_type)+
      ggtitle("Predictions")+
      theme_classic()+
      theme(panel.border=element_rect(fill=NA))
print(p)

if(use_covs==T){
  ## plot covariates
  for(cov in cov_list){
    p <- ggplot(data=df.mod)+
          geom_point(aes(x=get(cov), y=ldl))+
          xlab(cov)+
          theme_classic()
    print(p)
  }
  
  p <- ggplot
  sp <- stan_plot(fit, pars="beta")
  sp$data$params <- cov_list
  row.names(sp$data) <- cov_list
  print(sp)
}

dev.off()

## review week plots
alpha_named <- c("Only LDL", "TC", "TC & HDL", "TC, HDL, & TGL", "HDL", "HDL & TGL")
alpha_type <- c("a_", "a__chl", "a__chl_hdl", "a__chl_hdl_tgl", "a__hdl", "a__hdl_tgl")
ref <- data.table(alpha_named, alpha_type)

ldl_full <- merge(ldl_full, ref, by="alpha_type", all.x=T)
pdf(file=review_plot, width=11)
p <- ggplot(data=ldl_full[alpha_type!="a_"])+
      geom_point(aes(x=ldl, y=mean, color=alpha_named), size=1.8)+
      geom_abline(slope=1, intercept=0)+
      facet_wrap(~alpha_named)+
      xlab("Observed LDL")+
      ylab("Predicted LDL")+
      ggtitle("Single regression")+
      guides(color="none")+
      theme_classic()+
      theme(panel.border=element_rect(fill=NA), text=element_text(size=15))
print(p)

compare <- merge(ldl_full, pred_list, by="split_id", all.y=T)
#compare[, alpha:=factor(alpha, levels=unique(ldl_full$alpha_type))]
## show model fit for multiple regressions
p <- ggplot(data=compare[alpha==alpha_type & alpha!="a_"])+
      geom_point(aes(x=ldl, y=preds, color=alpha_named), size=1.8)+
      geom_abline(slope=1, intercept=0)+
      facet_wrap(~alpha_named)+
      theme_classic()+
      xlab("Observed LDL")+
      ylab("Predicted LDL")+
      ggtitle("Multiple regressions")+
      guides(color="none")+
      theme(panel.border=element_rect(fill=NA),text=element_text(size=15))
print(p)
dev.off()

################### SAVE RESULTS #########################################
## drop certain types
preds_full <- preds_full[!alpha_type %in% c("a_", "a__hdl", "a__hdl_tgl", "a__tgl")]
preds_full[, cv_cw:=1]
setnames(df.t, c("ldl", "se_ldl"), c("mean", "standard_error"))
setnames(preds_full, "sd", "standard_error")

## drop appropriate microdata rows
df.t <- df.t[!is.na(mean)]
df.t[, src_age_sex:=paste0(nid, "_", age_start, "_", sex_id)]
preds_full[, src_age_sex:=paste0(nid, "_", age_start, "_", sex_id)]
dupe_rws <- unique(df.t$src_age_sex)[unique(df.t$src_age_sex) %in% unique(preds_full$src_age_sex)]
preds_full <- preds_full[!src_age_sex %in% dupe_rws]


full <- rbind(df.t, preds_full, fill=T)

full <- full[, .(nid, file_path, smaller_site_unit, year_id, sex_id, age_start, age_end, ihme_loc_id, location_id, mean, standard_error, data_type, sample_size, cv_new, cv_cw, seq, parent_seqs)]
full <- full[mean>0]

## this is for rows w/o any SE, need SD to get imputed. SD column needs to be there even if its blank
if(!"standard_deviation" %in% names(full)){
  full[, standard_deviation:=NA]
}

################### SAVE RESULTS #########################################
invisible(sapply(c("nid", "year_id", "sex_id", "location_id"), check_class, df=full, class="integer"))
invisible(sapply(c("mean", "standard_error"), check_class, df=full, class="numeric"))
invisible(sapply(c("mean", "standard_error", "nid", "year_id", "sex_id", "age_start", "age_end", "location_id"), check_missing, full))

write.csv(full, file=output_path, row.names=F)
end <- Sys.time()
print(end-start)