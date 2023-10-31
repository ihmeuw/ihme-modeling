#### Empty the environment 
rm(list=ls())
windows <- Sys.info()[1][["sysname"]]=="Windows"
root <- ifelse(windows,"J:/","/home/j/")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))


### LIBRARIES
library(dplyr)
library(tidyr)
library(gtools)
library(data.table); library(readstata13); library(ggplot2)


### LOAD THE FUNCTION
repo_dir <-"FILEPATH"
source(paste0(repo_dir,"run_mr_brt_function.R"))
source(paste0(repo_dir,"cov_info_function.R"))
source(paste0(repo_dir,"check_for_outputs_function.R"))
source(paste0(repo_dir,"load_mr_brt_outputs_function.R"))
source(paste0(repo_dir,"predict_mr_brt_function.R"))
source(paste0(repo_dir,"check_for_preds_function.R"))
source(paste0(repo_dir,"load_mr_brt_preds_function.R"))
source(paste0(repo_dir,"plot_mr_brt_function.R"))

### OUTPUT DIR
output.dir <- paste0("FILEPATH")

### READ IN DATA (01d OUTPUT FROM BRADMOD DATA PREPARATION STEPS)
stata.path<-paste0("FILEPATH")
data<-as.data.table(read.dta13(stata.path))

### INFLATING 0s
data$meas_value[data$meas_value==0]<-10^(-15)

### DO LOGIT 
data$log_mean <- logit(data$meas_value)
data$meas_stdev<-as.numeric(data$meas_stdev)
data[, log_se := sqrt((1/(meas_value - meas_value^2))^2 * meas_stdev^2)]

### CALCULATE UNIQUE STUDY-ID AND MEDIAN CD4 
data$id <- ifelse(is.na(data$subcohort_id), data$nid, paste0(data$nid, data$subcohort_id))
data<-data[data$meas_stdev!=Inf,]
data$cd4_mid <- (data$age_lower*10 +data$age_upper*10)/2

### REFORMAT DURATION TIME 
data$time_point[data$time_point==6]<-"0_6"
data$time_point[data$time_point==12]<-"7_12"
data$time_point[data$time_point==24]<-"12_24"



ssa<-data[data$super=="ssa",]
other<-data[data$super=="other",]
high<-data[data$super=="high",]

### GBD 2017 BRADMOD RESULT
new_ssa <- read.csv("FILEPATH")
new_ssa$super<-"ssa"
new_other <-read.csv(paste0("FILEPATH")
new_other$super<-"other"
new_high<-read.csv("FILEPATH")
new_high$super <-"high"

### FORMAT 2017 DATA
new <- rbind(new_ssa,new_other,new_high)
new <-as.data.table(new)
new$time_point <- "0_6"
new$time_point[new$durationart=="6to12Mo"] <- "7_12"
new$time_point[new$durationart=="GT12Mo"] <- "12_24"

setnames(new,"age","age0")
new$age<-"15_25"
new$age[new$age0=="25-35"]<-"25_35"
new$age[new$age0=="35-45"]<-"35_45"
new$age[new$age0=="45-55"]<-"45_55"
new$age[new$age0=="55-100"]<-"55_100"

new[, prob := rowMeans(new[, grep("mort", names(new), value = T), with = F])]
new[, prob_lo := apply(new[, grep("mort", names(new), value = T), with = F], 1, quantile, probs = 0.025)]
new[, prob_hi := apply(new[, grep("mort", names(new), value = T), with = F], 1, quantile, probs = 0.975)]
new[, source := "GBD17"]

new$cd4_lower[new$cd4_lower==0]<-25
new$cd4_lower[new$cd4_lower==50]<-75
new$cd4_lower[new$cd4_lower==100]<-150
new$cd4_lower[new$cd4_lower==200]<-225
new$cd4_lower[new$cd4_lower==250]<-300
new$cd4_lower[new$cd4_lower==350]<-425
new$cd4_lower[new$cd4_lower==500]<-750
setnames(new,"cd4_lower","X_cd4_mid")

## MODEL DRAW2 DATA
output.dir<-"FILEPATH" 

### 3 REGIONS: R IS SSA/OTHER/HIGH
### WE NEED TO DO 1-(1-MORT)^2 FOR 0-6 AND 7-12 MONTH
r="ssa" 
for (a in c("15_25", "25_35","35_45","45_55","55_100")){
  for (s in c(1,2)){
    for ( t in c("0_6","7_12")){
      
      ### GET MODEL DRAW2 INPUT DATA
      file.path <- paste0("FILEPATH")
      gbd.dt<-read.csv(paste0(file.path,"model_draw2.csv"))
      
      ### KEPP ONLY THE LAST 1000 DRAWS
      gbd.dt<-gbd.dt[4001:5000,]
      gbd.dt$mort<-paste0("mort_",1:1000)
      
      ### RESHAPE: LONG TO WIDE
      gbd.l<-gather(gbd.dt, cd4_mid, mort, paste0(r,"_0"):paste0(r,"_500"), factor_key=TRUE)
      gbd.l$draw<- rep(paste0("draw",1:1000),7)
      w <- reshape(gbd.l, idvar = "cd4_mid", timevar = "draw", direction = "wide")
      
      w$age<-a
      w$sex<-s
      w$time_point<-t
      
      w<- as.data.table(w)
      w[, prob := rowMeans(w[, grep("mort.draw", names(w), value = T), with = F])]
      w[, prob_lo := apply(w[, grep("mort.draw", names(w), value = T), with = F], 1, quantile, probs = 0.025)]
      w[, prob_hi := apply(w[, grep("mort.draw", names(w), value = T), with = F], 1, quantile, probs = 0.975)]
      w[, source := "model_draw2"]
      
      w<-w %>% separate(cd4_mid, c("super", "cd4"))
      
      
      w$cd4_mid[w$cd4==0]<-25
      w$cd4_mid[w$cd4==50]<-75
      w$cd4_mid[w$cd4==100]<-150
      w$cd4_mid[w$cd4==200]<-225
      w$cd4_mid[w$cd4==250]<-300
      w$cd4_mid[w$cd4==350]<-425
      w$cd4_mid[w$cd4==500]<-750
      
      setnames(w,"cd4_mid","X_cd4_mid")
      w_mort <- w
      w_mort[,3:1002] <- 1-(1-w_mort[,3:1002])^2
      w_mort[, prob := rowMeans(w_mort[, grep("mort.draw", names(w_mort), value = T), with = F])]
      w_mort[, prob_lo := apply(w_mort[, grep("mort.draw", names(w_mort), value = T), with = F], 1, quantile, probs = 0.025)]
      w_mort[, prob_hi := apply(w_mort[, grep("mort.draw", names(w_mort), value = T), with = F], 1, quantile, probs = 0.975)]
      w_mort[, source := "model_draw2_mort"]
      
      ### ADJUST DATA POINTS BY HERE 
      dt <- data[super == r & age==a  & time_point==t & sex==s, ]
      #dt <- dt[!(dt$meas_value>=0.25),]
      fit <- run_mr_brt(
        output_dir = output.dir,
        model_label = "test_o",
        data = dt,
        mean_var = "log_mean",
        se_var = "log_se",
        covs = list(
          cov_info("cd4_mid", "X",
                   degree = 3,
                   ### ADD KNOTS 
                   i_knots =  "50,100,150, 250, 400", 
                   bspline_cvcv = "convex",
                   bspline_mono = "decreasing"
          )),
        study_id = "id",
        overwrite_previous = TRUE
        , trim_pct = 0.1 ## TRIMMING PERCENT 
        , method = "trim_maxL"
      )
      
      df_pred <- data.frame(
        cd4_mid = c(25, 75, 150, 225 , 300, 425, 750))
      pred1 <- predict_mr_brt(fit, newdata = df_pred,write_draws=T)
      
      draws <- as.data.table(pred1$model_draws)
      draws$time<-t
      draws$sex<-s
      draws$age<-a
      ### COVERT TO NORMAL SPACE
      draws[,4:1003]<-exp(draws[,4:1003])/(1+exp(draws[,4:1003]))
      
      draws[, prob := rowMeans(draws[, grep("draw", names(draws), value = T), with = F])]
      draws[, prob_lo := apply(draws[, grep("draw", names(draws), value = T), with = F], 1, quantile, probs = 0.025)]
      draws[, prob_hi := apply(draws[, grep("draw", names(draws), value = T), with = F], 1, quantile, probs = 0.975)]
      draws$source<-"MR_BRT"
      
      draw <- as.data.table(pred1$model_draws)
      draw$time<-t
      draw$sex<-s
      draw$age<-a
      draw[,4:1003]<-exp(draw[,4:1003])/(1+exp(draw[,4:1003]))
      draw[,4:1003]<-1-(1-draw[,4:1003])^2
      
      ### SAVE OUTPUT 
      write.csv(draw,paste0(output.dir,r,"_res/",r,"_",a,"_",s,"_",t,".csv"))
      
      draw[, prob := rowMeans(draw[, grep("draw", names(draw), value = T), with = F])]
      draw[, prob_lo := apply(draw[, grep("draw", names(draw), value = T), with = F], 1, quantile, probs = 0.025)]
      draw[, prob_hi := apply(draw[, grep("draw", names(draw), value = T), with = F], 1, quantile, probs = 0.975)]
      
      
      draw$source<-"MR_BRT_mort"
      
      gbd17<-new[new$super==r & new$sex==s & new$time_point==t & new$age==a]
      gbd17[, source := "GBD17"]
      
      comb <- rbind(gbd17, draws, use.names = T, fill = T)
      comb1<- rbind(draw, comb, use.names = T, fill = T)
      comb.dt <- rbind(w, comb1, use.names = T, fill = T)
      
      comb <- rbind(gbd17, w, use.names = T, fill = T)
      comb.dt<- rbind(w_mort, comb, use.names = T, fill = T)
      comb.dt <- rbind(w, comb1, use.names = T, fill = T)
      
      
      mod_data <- as.data.table(fit$train_data)
      mod_data[, outlier := floor(abs(w-1))]
      mod_data[, mean := exp(log_mean)/(1+exp(log_mean))]
      
      ### PLOTING THE COMPARISON FIGURES
      pdf(paste0(output.dir,r, "/model_draw2_",r,"_",s,"_",a,"_",t,"m.pdf"))
      gg <- ggplot(data = comb.dt, aes(x = X_cd4_mid)) +
        geom_ribbon(aes(ymin = prob_lo, ymax = prob_hi, fill = source), alpha = 0.2) + 
        geom_point(data=mod_data, aes(x=cd4_mid, y=mean, size=1/log_se^2, shape=factor(outlier))) +  scale_shape_manual(values=c(17, 22))+
        geom_line(aes(y = prob, color = source)) +
        ggtitle(paste0("Region:", r,"; Sex :",s, "; Age:",a, "; Time:",t )) +
        #  ylim(0, 0.2) +
        theme_bw()
      print(gg)
      dev.off()
      
    }
  }
}


output.dir<- paste0("FILEPATH")   

### 13-24 MONTH
r="high" 
for (a in c("15_25", "25_35","35_45","45_55","55_100")){
  for (s in c(1,2)){
    for ( t in c("12_24")){
      
      file.path <- paste0(root,"FILEPATH",a,"_",s,"_",t,"_",r,"/FILEPATH/")
      gbd.dt<-read.csv(paste0(file.path,"model_draw2.csv"))
      
      
      gbd.dt<-gbd.dt[4001:5000,]
      gbd.dt$mort<-paste0("mort_",1:1000)
      
      gbd.l<-gather(gbd.dt, cd4_mid, mort, paste0(r,"_0"):paste0(r,"_500"), factor_key=TRUE)
      gbd.l$draw<- rep(paste0("draw",1:1000),7)
      w <- reshape(gbd.l, idvar = "cd4_mid", timevar = "draw", direction = "wide")
      
      w$age<-a
      w$sex<-s
      w$time_point<-t
      
      w<- as.data.table(w)
      w[, prob := rowMeans(w[, grep("mort.draw", names(w), value = T), with = F])]
      w[, prob_lo := apply(w[, grep("mort.draw", names(w), value = T), with = F], 1, quantile, probs = 0.025)]
      w[, prob_hi := apply(w[, grep("mort.draw", names(w), value = T), with = F], 1, quantile, probs = 0.975)]
      w[, source := "model_draw2"]
      
      w<-w %>% separate(cd4_mid, c("super", "cd4"))
      
      
      
      w$cd4_mid[w$cd4==0]<-25
      w$cd4_mid[w$cd4==50]<-75
      w$cd4_mid[w$cd4==100]<-150
      w$cd4_mid[w$cd4==200]<-225
      w$cd4_mid[w$cd4==250]<-300
      w$cd4_mid[w$cd4==350]<-425
      w$cd4_mid[w$cd4==500]<-750
      
      setnames(w,"cd4_mid","X_cd4_mid")
      
      
      dt <- data[super == r & age==a  & time_point==t & sex==s, ]
      dt<- dt[!(dt$meas_value>=0.04),]
      
      fit <- run_mr_brt(
        output_dir = output.dir,
        model_label = "test",
        data = dt,
        mean_var = "log_mean",
        se_var = "log_se",
        covs = list(
          cov_info("cd4_mid", "X",
                   degree = 3,
                   i_knots =  "50,100,150, 250, 400",
                   bspline_cvcv = "convex"
                   ,bspline_mono = "decreasing"
          )),
        study_id = "id",
        overwrite_previous = TRUE,
        ### TRIMMING PERCENT
        trim_pct = 0.1,
        method = "trim_maxL"
      )
      
      df_pred <- data.frame(
        cd4_mid = c(25, 75, 150, 225 , 300, 425, 750))
      pred1 <- predict_mr_brt(fit, newdata = df_pred,write_draws=T)
      
      draws <- as.data.table(pred1$model_draws)
      draws$time<-t
      draws$sex<-s
      draws$age<-a
      draws[,4:1003]<-exp(draws[,4:1003])/(1+exp(draws[,4:1003]))
      
      
      draws[, prob := rowMeans(draws[, grep("draw", names(draws), value = T), with = F])]
      draws[, prob_lo := apply(draws[, grep("draw", names(draws), value = T), with = F], 1, quantile, probs = 0.025)]
      draws[, prob_hi := apply(draws[, grep("draw", names(draws), value = T), with = F], 1, quantile, probs = 0.975)]
      draws$source<-"MR_BRT"
      
      draw <- as.data.table(pred1$model_draws)
      draw$time<-t
      draw$sex<-s
      draw$age<-a
      draw[,4:1003]<-exp(draw[,4:1003])/(1+exp(draw[,4:1003]))
      draw[, prob := rowMeans(draw[, grep("draw", names(draw), value = T), with = F])]
      draw[, prob_lo := apply(draw[, grep("draw", names(draw), value = T), with = F], 1, quantile, probs = 0.025)]
      draw[, prob_hi := apply(draw[, grep("draw", names(draw), value = T), with = F], 1, quantile, probs = 0.975)]
      
      write.csv(draw,paste0(output.dir,r,"_res/",r,"_",a,"_",s,"_",t,".csv"))
      
      draw$source<-"MR_BRT_mort"
      
      gbd17<-new[new$super==r & new$sex==s & new$time_point==t & new$age==a]
      gbd17[, source := "GBD17"]
      comb <- rbind(gbd17, draws, use.names = T, fill = T)
      comb1<- rbind(draw, comb, use.names = T, fill = T)
      comb.dt <- rbind(w, comb1, use.names = T, fill = T)
      
      mod_data <- as.data.table(fit$train_data)
      mod_data[, outlier := floor(abs(w-1))]
      mod_data[, mean := exp(log_mean)/(1+exp(log_mean))]
      
      pdf(paste0(output.dir,r, "/model_draw2_",r,"_",s,"_",a,"_",t,"m.pdf"))
      gg <- ggplot(data = comb.dt, aes(x = X_cd4_mid)) +
        geom_ribbon(aes(ymin = prob_lo, ymax = prob_hi, fill = source), alpha = 0.2) + #color = "#56B4E9"
        geom_point(data=mod_data, aes(x=cd4_mid, y=mean, size=1/log_se^2, shape=factor(outlier))) +  scale_shape_manual(values=c(17, 22))+
        geom_line(aes(y = prob, color = source)) +
        ggtitle(paste0("Region:", r,"; Sex :",s, "; Age:",a, "; Time:",t )) +
        #  ylim(0, 0.2) +
        theme_bw()
      print(gg)
      dev.off()
    }
  }
}