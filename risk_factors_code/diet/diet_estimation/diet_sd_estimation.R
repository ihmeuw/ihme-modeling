rm(list = ls())
if (Sys.info()["sysname"] == "Darwin") j_drive <- "/Volumes/snfs"
if (Sys.info()["sysname"] == "Linux") j_drive <- "/home/j"
user <- Sys.info()[["user"]]

library(rio)
library(dplyr)

  input <- fread(paste0(j_drive,"/temp/",user,"/diet_datasets/alldiet_cleaned_nat_rep_fatsupdated_20170707.csv"))
  ## gold standard data for regression
  input <- input %>% dplyr::filter(cv_FFQ!=1 & cv_cv_hhbs!=1 & cv_sales_data!=1 & cv_fao!=1)
  ## sd from se
  input <- input %>% dplyr::mutate(standard_deviation=ifelse(is.na(standard_deviation),standard_error*sqrt(sample_size),standard_deviation))
  input <- input %>% dplyr::mutate(outlier=ifelse(((standard_deviation / mean > 2) | (standard_deviation / mean < .1)),1,0))
  input <- input %>% dplyr::filter(outlier==0)
  input$standard_deviation <- as.numeric(input$standard_deviation)
  input$mean <- as.numeric(input$mean)

for(r in unique(input$ihme_risk)) {
      input_f <- input %>% dplyr::filter(ihme_risk==r)
      reg <- lm(log(standard_deviation) ~ log(mean), input_f)
      ## review plot
      pdf(paste0(j_drive,"/temp/",user,"/GBD_2016/calc_paf/parallel/mean_sd_reg_",r,".pdf"))
      with(input_f,plot(log(mean), log(standard_deviation)),main=paste0(r," sd-mean reg"))
      abline(reg)
      dev.off()
      print(paste0("RISK: ",r))
      print(summary(reg))
      saveRDS(reg,paste0(j_drive,"/temp/",user,"/GBD_2016/calc_paf/parallel/mean_sd_reg_",r,".rdata"))
}
 