

if (Sys.info()['sysname'] == 'Linux') {
  require(MASS)
  require(magrittr)
  require(data.table)
  library(glmnet)
} else {
  require(pacman)
  p_load(glmnet,data.table,dplyr,magrittr,arm,blme,DT,mvtnorm,merTools,devtools,readstata13,foreign,lme4)
}

j_root <- "FILEPATH"

args<-commandArgs(trailingOnly = TRUE)
como<-args[1]
output_path_summary_stats<-args[2]
output_path_bootstrap<-args[3]

Sys.info()
sessionInfo()

logit<-function(x){
  x<-log((x)/(1-x))
  return(x)
}

inv.logit<-function(x){
  x<-1/(1+exp(-x))
  return(x)
}

set.seed(555)

## bring in the datasets
interpolation <- fread(paste0(j_root,"FILEPATH.csv"))
interpolation[,key:=as.numeric(key)]
interpolation[,sf:=round(sf,digits = 5)]
interpolation[,dw:=round(dw,digits=5)]
interpolation[,predict:=round(predict,digits = 5)]

Appended_MEPS_DW_ordered <- fread(paste0(j_root,"FILEPATH.csv"))
Appended_MEPS_DW_ordered[,key:=as.numeric(key)]
Appended_MEPS_DW_ordered[,sf:=round(sf,digits = 5)]
Appended_MEPS_DW_ordered[,dw:=round(dw,digits=5)]
Appended_MEPS_DW_ordered[,predict:=round(predict,digits = 5)]

data <- merge(interpolation, Appended_MEPS_DW_ordered, by = c("key",'dw','predict','sf'), all=T,with=F)

# drop all rows missing dw, these are not real observations - just an artifact of the crosswalk step
data<-data[!is.na(dw_hat),]

# some dws were modeled as less than zero or more than 1, we truncate them here. Furthermore, we use a logit transformation,
# so these need to be slightly apart from 1 and 0
data$dw_hat <- ifelse(data$dw_hat > 0.999999, .999999, ifelse(data$dw_hat < 0.000001, .000001, data$dw_hat))

# make a logit transformed DW, to be used as the response variable in the model
data[,logit_dw_hat:=logit(dw_hat)]

# replace spaces in variable names with _, replaces commas with nothing, and - with _
setnames(data,names(data),gsub(" ","_",names(data)))
setnames(data,names(data),gsub(",","",names(data)))
setnames(data,names(data),gsub("-","_",names(data)))
setnames(data,names(data),gsub("\\(","",names(data)))
setnames(data,names(data),gsub("\\)","",names(data)))
setnames(data,names(data),gsub("/","_",names(data)))
setnames(data,names(data),tolower(names(data)))
comos<-grep('^t',names(data),value=T)

# create a list of comos to predict for
comos_to_predict<-fread(paste0(j_root,"FILEPATH.csv"))
comos_to_predict<-comos_to_predict[!yld_cause==""]

# create map from source to MEPS dummy var
source<-unique(comos_to_predict[,source])
meps_dummy_map<-data.table(source)
meps_dummy_map[grep('MEPS',source),"MEPS":=1]
meps_dummy_map[is.na(MEPS)==T,MEPS:=0]

# merge dummy map onto comos_to_predict, only keep those that use MEPS as sources
comos_to_predict<-merge(comos_to_predict,meps_dummy_map,by="source")
comos_to_predict<-comos_to_predict[MEPS==1]
comos_to_predict[,comos:=paste0("t",yld_cause)]
comos_to_predict<-tolower(unique(comos_to_predict[,comos]))

# convert condition indicators to numeric variables
data[, (comos):=lapply(.SD, as.numeric), .SD=comos]

# drop rows with NAs for conditions
data<-data[complete.cases(data[,comos,with=F]),]
data[,id:=as.numeric(id)]

# drop conditions that only have value of 0
for (c in comos){
  if (max(data[,get(c)])==0){
    data[,(c):=NULL]
    cond<-gsub(pattern = "^t",replacement = "",x = c)
    print(paste0("dropped ", cond, " from data becasue there were no observations"))
  }
}

comos<-grep('^t',names(data),value=T)

# the model
x<-as.matrix(data[,c(comos),with=F])
print(class(x))
y<-as.matrix(data[,.(logit_dw_hat)])
print(class(y))
cvfit<-cv.glmnet(x,y)
lasso_selection<-data.table(condition=dimnames(coef(cvfit,s=cvfit$lambda.min))[[1]],
                            beta=as.matrix(coef(cvfit,s=cvfit$lambda.min))[,1])
comos_lasso<-lasso_selection[beta!=0&condition!="(Intercept)",condition]

# add back on any conditions we need that got dropped
comos_add<-setdiff(comos_to_predict,comos_lasso)
print(paste("adding back on",paste(comos_add,collapse = " ")))
comos_lasso<-append(comos_lasso,comos_add)

# Drop comos not being used
drop<-comos[which(!comos%in%comos_lasso,useNames = T)]
print(paste0("dropping ", drop, " from model. elimiated by lasso regression"))
data[,(drop):=NULL]

fixed<-paste("logit_dw_hat ~ ",paste(comos_lasso,collapse='+'),sep="")
model<-lm(as.formula(paste(fixed)),data=data)


#Perform the analysis
    print(paste("currently looping through",como))

    # keep only those with the conditionin question
    data.como<-data[get(noquote(como))==1]

    # create matrix of 1000 draws of beta values from standard error, assuming normally distributed errors
    # (nrow=number of betas and ncol=number of draws)
    coefs<-coef(summary(model))[,1]
    covmat<-vcov(model)
    predmat<-t(mvrnorm(n = 1000, mu = coefs, Sigma = covmat))

    # create matrix of x values to multiply by coefficient matrix
    x_values<-data.como[,c(comos_lasso),with=F]
    x_values[,"(Intercept)":=1]
    setcolorder(x=x_values,neworder = variable.names(model))
    x_values<-as.matrix(x_values)

    # predict for their DW and reverse logit it
    dw_obs<-x_values%*%predmat
    dw_obs<-as.data.table(dw_obs)%>%inv.logit()
    dw_obs_cols<-paste0("dw_obs_",0:999)
    data.como[,(dw_obs_cols):=dw_obs]
    print(paste("finished predicting DW for",como))

    # replace the condition in question to zero
    data.como[,(como):=0]

    # again, create matrix of x values to multiply by coefficient matrix
    x_values<-data.como[,c(comos_lasso),with=F]
    x_values[,"(Intercept)":=1]
    setcolorder(x=x_values,neworder = variable.names(model))
    x_values<-as.matrix(x_values)

    # now predict and inverse logit again. This will give the counterfactual DW (or their expected weight if they didnt have the condition in question)
    dw_s<-x_values%*%predmat
    dw_s<-as.data.table(dw_s)%>%inv.logit()
    dw_s_cols<-paste0("dw_s_",0:999)
    data.como[,(dw_s_cols):=dw_s]
    print("finsihed predicting counterfactual")

    # get the mean of the predictions above for the population with the condition
    mean_dw_s<-colMeans(data.como[,dw_s_cols,with=F])
    mean_dw_obs<-colMeans(data.como[,dw_obs_cols,with=F])

    # estimate the effect of the condition in question while correcting for comorbidities via the COMO equation
    dw_obs_inv<-1-dw_obs
    dw_s_inv<-1-dw_s
    div_draws<-dw_obs_inv/dw_s_inv
    div_draws<-1-div_draws

    # get the mean of this condition specific disability weight.
    dw_t_cols<-paste0('dw_t_',0:999)
    data.como[,(dw_t_cols):=div_draws]
    mean_dw_t<-colMeans(data.como[,dw_t_cols,with=F])
    se<-apply(data.como[,dw_t_cols,with=F],2,sd)

    condition.stats<-data.table(mean_dw_t,se,mean_dw_obs,mean_dw_s)
    condition.stats[,N:=nrow(data.como)]
    condition.stats[,como_name:=como]
    condition.stats[,dependent:="logit"]
    condition.stats.melt<-melt(condition.stats,id.vars = c("N","como_name","dependent"))
    condition.stats.melt[,draw_num:=rep(0:999,4)]
    condition.stats.wide<-dcast(condition.stats.melt,N+como_name+dependent~variable+draw_num,variable.names=c("variable","draw_num"),value.var = "value")

    # save summary statistics
    dir.create(paste0(j_root,'FILEPATH',output_path_summary_stats,'/'),recursive = T)
    write.csv(condition.stats.wide,file=paste0(j_root,'FILEPATH',output_path_summary_stats,'/',como,'.csv'),row.names = F)

    data.como<-data.como[,c('id','sex','age','region','datayear','education','personal_income','dw_hat',dw_obs_cols,dw_s_cols,dw_t_cols),with=F]
    setnames(data.como, 'dw_hat','DW_data')
    dw_pred_cols<-paste0("DW_pred_",0:999)
    setnames(data.como, dw_obs_cols,dw_pred_cols)
    dw_counter_cols<-paste0("DW_counter_",0:999)
    setnames(data.como, dw_s_cols,dw_counter_cols)
    dw_diff_pred_cols<-paste0("DW_diff_pred_",0:999)
    setnames(data.como, dw_t_cols, dw_diff_pred_cols)
    melted.data.como<-melt(data.como,id.vars = c(dw_pred_cols,dw_diff_pred_cols,'id','region','sex','education','personal_income','DW_data',value_var=dw_counter_cols))
    dw_diff_data_cols<-paste0("DW_diff_data_",0:999)
    data.como[,(dw_diff_data_cols):= lapply(0:999, function(x) 1- ((1- DW_data)/(1- get(data.como[,paste0('DW_counter_',x)]))))]

    # save bootstrap dataset
    dir.create(paste0(j_root,'FILEPATH',output_path_bootstrap,"/"),recursive = T)
    write.csv(data.como,file=paste0(j_root,'FILEPATH',output_path_bootstrap,'/',como,'.csv'),row.names = F)
