os <- .Platform$OS.type
if (os == "windows") {
 source("FILEPATH")
} else {
 source("FILEPATH")
}

library(metafor, lib.loc=fix_path("ADDRESS"))
library(msm)
library(plyr)
library(boot)
library(data.table)
library(ggplot2)
library(dplyr)

source(fix_path("FILEPATH"))
source(fix_path("FILEPATH"))
source(fix_path("FILEPATH"))
source(fix_path("FILEPATH"))

atx <- as.data.table(read.csv(fix_path("FILEPATH")))

ggplot(subset(atx, var!="lri_tx"), aes(x=var, y=mean, col=var)) + geom_boxplot() + geom_point(alpha=0.5) + theme_bw() +
  scale_y_log10()

df <- data.table(atx)
df <- df[which(df$var!="lri_tx"),]
## Levels
levels <- c(1, 2, 3, 4)

###########################################################################################################################
# 														clean and prep data
###########################################################################################################################

## Subset
# keep <- c('nid', 'survey_name', 'ihme_loc_id', 'year_start', 'year_end', 'survey_module', 'file_path',
#           'data', 'variance', 'sample_size', 'age_group_id', 'year_id',
#           'me_name', 'bundle')

# df <- df[, keep, with=FALSE]

## Generate id for utility
cols <- c('nid', 'survey_name', 'ihme_loc_id', 'year_start', 'year_end', 'survey_module', 'file_path',"age_start","age_end")
df <- df[, id := .GRP, by=cols]
## Store meta
meta <- df[, c(cols, "id"), with=FALSE] %>% unique
df <- df[, -c(cols), with=FALSE]

#patch: removing duplicate rows
df <- df[-which(duplicated(df, by=c("id","var"))),]

## Check for duplicates by "id" and "me_name"
#check <- df[duplicated(df, by=c("id", "me_name"))]
check <- df[duplicated(df, by=c("id", "var"))]
if (nrow(check) > 0) stop("Duplicates detected")

## Remove zeros
df <- df[which(df$mean != 0),]

#patch: convert all mean==1 to mean==0.99999
df[which(df$mean==1),"mean"] <- 0.99999

## Reshape wide
df.w <- dcast(df, id ~ var, value.var = c("mean", "standard_error","sample_size"))

## Keep only reference data
  df.w <- data.frame(df.w)
  rdf <- subset(df.w, !is.na(mean_lri1))

## rename so consistent with LRI prevalence code
  setnames(rdf, c("mean_lri1","mean_lri2","mean_lri3","mean_lri4"), c("chest_fever","chest_symptoms","diff_fever","diff_breathing"))
  setnames(rdf, c("standard_error_lri1","standard_error_lri2","standard_error_lri3","standard_error_lri4"),
           c("chest_fever_std","chest_symptoms_std","diff_fever_std","diff_breathing_std"))

## Define ratios of prevalence in various definitions
  rdf$chest_fever_chest <- rdf$chest_fever / rdf$chest_symptoms
  rdf$chest_fever_diff_fever <- rdf$chest_fever / rdf$diff_fever
  rdf$chest_fever_diff <- rdf$chest_fever / rdf$diff_breathing
  rdf$diff_fever_diff <- rdf$diff_fever / rdf$diff_breathing
  rdf$chest_diff <- rdf$chest_symptoms / rdf$diff_breathing

## Define standard error for each ratio
  rdf$chest_fever_chest_se <- sqrt(rdf$chest_fever^2 / rdf$chest_symptoms^2 * (rdf$chest_fever_std^2/rdf$chest_symptoms^2 + rdf$chest_symptoms_std^2/rdf$chest_fever^2))
  rdf$chest_fever_diff_fever_se <- sqrt(rdf$chest_fever^2 / rdf$diff_fever^2 * (rdf$chest_fever_std^2/rdf$diff_fever^2 + rdf$diff_fever_std^2/rdf$chest_fever^2))
  rdf$chest_fever_diff_se <- sqrt(rdf$chest_fever^2 / rdf$diff_breathing^2 * (rdf$chest_fever_std^2/rdf$diff_breathing^2 + rdf$diff_breathing_std^2/rdf$chest_fever^2))
  rdf$chest_diff_se <- sqrt(rdf$chest_symptoms^2 / rdf$diff_breathing^2 * (rdf$chest_symptoms_std^2/rdf$diff_breathing^2 + rdf$diff_breathing_std^2/rdf$chest_symptoms^2))

## the network meta-analysis must be long (currently wide)
## and must have dummies for each of the definitions that are crosswalked.
rdf_chest_chest <- rdf[,c("id","chest_fever_chest","chest_fever_chest_se")]
rdf_chest_diff_fever <- rdf[,c("id","chest_fever_diff_fever","chest_fever_diff_fever_se")]
rdf_chest_diff <- rdf[,c("id","chest_fever_diff","chest_fever_diff_se")]

# rename
colnames(rdf_chest_chest)[2:3] <- c("ratio","standard_error")
colnames(rdf_chest_diff_fever)[2:3] <- c("ratio","standard_error")
colnames(rdf_chest_diff)[2:3] <- c("ratio","standard_error")

# set dummy indicator
rdf_chest_chest$indictor <- "chest"
rdf_chest_diff_fever$indictor <- "diff_fever"
rdf_chest_diff$indictor <- "diff"

cv_lri_tx <- rbind(rdf_chest_chest, rdf_chest_diff_fever, rdf_chest_diff)

# Create dummies for meta-analysis
cv_lri_tx$cv_chest <- ifelse(cv_lri_tx$indictor=="chest",1,0)
cv_lri_tx$cv_diff_fever <- ifelse(cv_lri_tx$indictor=="diff_fever",1,0)
cv_lri_tx$cv_diff <- ifelse(cv_lri_tx$indictor=="diff",1,0)

# remove zeros and NAs
cv_lri_tx <- subset(cv_lri_tx, ratio>0)
#cv_lri_tx <- subset(cv_lri_tx, standard_error!="NaN")
cv_lri_tx <- subset(cv_lri_tx, standard_error!="NaN"&standard_error!=0)

# ratios in log space
cv_lri_tx$log_ratio <- log(cv_lri_tx$ratio)

cv_lri_tx$log_se <- sapply(1:nrow(cv_lri_tx), function(i) {
  ratio_i <- cv_lri_tx[i, "ratio"]
  ratio_se_i <- cv_lri_tx[i, "standard_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})


fit2 <- run_mr_brt(
  output_dir = fix_path("ADDRESS"),
  model_label = "lri_tx_chest",
  data = cv_lri_tx[cv_lri_tx$cv_chest==1,],
  mean_var = "log_ratio",
  se_var = "log_se",
  overwrite_previous = TRUE,
  study_id = "id"
)
fit3 <- run_mr_brt(
  output_dir = fix_path("ADDRESS"),
  model_label = "lri_tx_diff_fever",
  data = cv_lri_tx[cv_lri_tx$cv_diff_fever==1,],
  mean_var = "log_ratio",
  se_var = "log_se",
  overwrite_previous = TRUE,
  study_id = "id"
)
fit4 <- run_mr_brt(
  output_dir = fix_path("ADDRESS"),
  model_label = "lri_tx_diff",
  data = cv_lri_tx[cv_lri_tx$cv_diff==1,],
  mean_var = "log_ratio",
  se_var = "log_se",
  overwrite_previous = TRUE,
  study_id = "id"
)

check_for_outputs(fit2)
check_for_outputs(fit3)
check_for_outputs(fit4)

df_pred <- data.frame(intercept=1)
# Chest
pred2 <- predict_mr_brt(fit2, newdata = df_pred)
check_for_preds(pred2)
pred_object <- load_mr_brt_preds(pred2)
preds_chest <- pred_object$model_summaries
# Diff + fever
pred3 <- predict_mr_brt(fit3, newdata = df_pred)
check_for_preds(pred3)
pred_object <- load_mr_brt_preds(pred3)
preds_diff_fever <- pred_object$model_summaries
# Just diff
pred4 <- predict_mr_brt(fit4, newdata = df_pred)
check_for_preds(pred4)
pred_object <- load_mr_brt_preds(pred4)
preds_diff <- pred_object$model_summaries

## Collect outputs
survey_outputs <- data.frame(log_chest_ratio = preds_chest$Y_mean,
                             log_diff_fever_ratio = preds_diff_fever$Y_mean,
                             log_diff_ratio = preds_diff$Y_mean,
                             log_chest_se = (preds_chest$Y_mean_hi - preds_chest$Y_mean_lo)/2/qnorm(0.975),
                             log_diff_fever_se = (preds_diff_fever$Y_mean_hi - preds_diff_fever$Y_mean_lo)/2/qnorm(0.975),
                             log_diff_se = (preds_diff$Y_mean_hi - preds_diff$Y_mean_lo)/2/qnorm(0.975))

df.w$indicator <- ifelse(!is.na(df.w$mean_lri1),"Reference",ifelse(!is.na(df.w$mean_lri2),"Alt 1",ifelse(!is.na(df.w$mean_lri3),"Alt 2","Alt 3")))

df.w <- cbind(df.w, survey_outputs)

# Log transform means, standard errors, to be consistent with the log-transformed ratios
for(v in c("lri2","lri3","lri4")){
  df.w[,paste0("log_mean_",v)] <- log(df.w[,paste0("mean_",v)])

  df.w[,paste0("log_se_",v)] <- sapply(1:nrow(df.w), function(i) {
    ratio_i <- df.w[i, paste0("mean_",v)]
    ratio_se_i <- df.w[i, paste0("standard_error_",v)]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })
}

# Crosswalk the results #
xw_lri2 <- df.w$log_mean_lri2 + df.w$log_chest_ratio
se_xw_lri2 <- sqrt(df.w$log_se_lri2^2 + df.w$log_chest_se^2)

xw_lri3 <- df.w$log_mean_lri3 + df.w$log_diff_fever_ratio
se_xw_lri3 <- sqrt(df.w$log_se_lri3^2 + df.w$log_diff_fever_se^2)

xw_lri4 <- df.w$log_mean_lri4 + df.w$log_diff_ratio
se_xw_lri4 <- sqrt(df.w$log_se_lri4^2 + df.w$log_diff_se^2)

df.w$data <- with(df.w, ifelse(indicator=="Reference", mean_lri1, ifelse(indicator=="Alt 1", exp(xw_lri2), ifelse(indicator=="Alt 2", exp(xw_lri3), exp(xw_lri4)))))

# Get the log-transformed SE for each row, transform to linear
log_ratio <- log(df.w$data)
final_se <- ifelse(df.w$indicator=="Reference", df.w$standard_error_lri1, ifelse(df.w$indicator=="Alt 1", se_xw_lri2,ifelse(df.w$indicator=="Alt 2", se_xw_lri3, se_xw_lri4)))
final_se <- sapply(1:nrow(df.w), function(i) {
  ratio_i <- log_ratio[i]
  ratio_se_i <- final_se[i]
  deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})

df.w$standard_error <- ifelse(df.w$indicator=="Reference", df.w$standard_error_lri1, final_se)
df.w$variance <- df.w$standard_error^2


###########################################################################################################################
# 														Crosswalk
###########################################################################################################################

## Regressions
# mods <- lapply(levels[2:4], function(x) {
#   form <- paste0("logit(mean_lri1)~logit(mean_lri", x, ")") %>% as.formula
#   lm(form, data=df.w,na.action=na.omit)
# })

#df.w[which(!is.na(df.w$mean_lri1)&!is.na(df.w$mean_lri2)),]$mean_lri1
mods <- lapply(levels[2:4],function(x){
  form <- paste0("logit(mean_lri1)~logit(mean_lri", x, ")") %>% as.formula
  dat_temp <- as.data.frame(df.w)
  col1 <- "mean_lri1"
  col2 <- paste0("mean_lri",x)
  dat_temp <- dat_temp[,c("mean_lri1",paste0("mean_lri",x))]
  dat_temp <- dat_temp[which(!is.na(dat_temp[,1])&!is.na(dat_temp[,2])),]
  lm(form,data=dat_temp)
})

#df.w[which(!is.na(df.w$mean_lri1)&!is.na(df.w$mean_lri2)),]
names(mods) <- paste0("lm", levels[2:4])

## Predict
df.w <- as.data.table(df.w)
df.w <- df.w[, predict_lri2 := predict(mods[['lm2']], newdata=df.w) %>% inv.logit]
df.w <- df.w[, predict_lri3 := predict(mods[['lm3']], newdata=df.w) %>% inv.logit]
df.w <- df.w[, predict_lri4 := predict(mods[['lm4']], newdata=df.w) %>% inv.logit]

## Fill in estimate based on priority (lri1 > lri2 > lri3...)
df.w$data <- as.numeric(NA)
df.w$sample_size <- as.integer(NA)
for (i in levels) {
  ## Settings
  fill <- paste0("predict_lri", i)
  fill2 <- paste0("sample_size_lri",i)
  if (i == 1) fill <- "mean_lri1"
  var <- paste0("standard_error_lri", i)
  mod <- paste0("lm", i)
  ## Store what definition is used
  df.w <- df.w[is.na(data) & !is.na(get(fill)), cv_cw := paste0("lri", i)]
  ## Propagate variance from model (new variance = data variance + beta_variance*data^2 + intercept_variance)
  if (i > 1) {
    beta_variance <-coef(summary(mods[[mod]]))[2,2]**2
    intercept_variance <- coef(summary(mods[[mod]]))[1,2]**2
    df.w <- df.w[is.na(data) & !is.na(get(fill)), variance := get(var)^2 + beta_variance*get(fill)^2 + intercept_variance]
  } else {
    df.w <- df.w[, variance := get(var)^2]
  }
  ## Store the value
  df.w <- df.w[is.na(data) & !is.na(get(fill)), c("data","sample_size") := list(get(fill),get(fill2))]
}

###########################################################################################################################
# 														Clean and output
###########################################################################################################################

## Clean data
out_cols <- c("id","data","sample_size","variance","cv_cw")
out <- data.frame(df.w)[, out_cols]
out$orig_mean <- sapply(1:nrow(out),function(x){
  col <- paste0("mean_",out[x,"cv_cw"])
  return(data.frame(df.w)[x,col])
})
out$orig_variance <- sapply(1:nrow(out),function(x){
  col <- paste0("standard_error_",out[x,"cv_cw"])
  return((data.frame(df.w)[x,col])^2)
})
out <- out[which(out$data != 0),] ## Dropping rows with 0

## Merge onto metadata
save <- merge(meta, out, by='id')
save <- save[, id := NULL]
save <- save[, me_name := "lri"]

## Temp fix
#save <- save[, sample_size := NA]

# ## Model prep and save
# save <- model_prep(save)

# Save data to /Crosswalked
outpath = fix_path("ADDRESS")
fullpath <- paste0(outpath,"FILEPATH")
write.csv(save,file=fullpath)
return(save)

ggplot(df.w, aes(x=cv_cw, y=data, col=cv_cw)) + geom_boxplot() + geom_point()
