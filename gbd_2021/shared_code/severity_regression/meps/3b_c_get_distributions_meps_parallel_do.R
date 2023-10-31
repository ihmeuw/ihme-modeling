########################################################################
### Project:  MEPS Severity Analysis
### Purpose:  Submitted by 3b_a to be run in parallel on the cluster.
###           Calculates severity proportions by cause.
########################################################################

library(data.table)

  j <- 'FILEPATH'

args<-commandArgs(trailingOnly = TRUE)
cause<-args[1]


# load severity cutoffs
data <- fread(paste0(j, "FILEPATH.csv"))

## make 1000 distribution variables
data <- data[yld_cause==cause,]

for(d in c(0:999)){
  data[,paste0("dist", d):=as.numeric(NA)]
}

no_asymptomatic <- c("acne", "digest_bile", "digest_hernia", "digest_pancreatitis", "inf_uri", "msk_pain_lowback_noleg", "msk_pain_lowback_wleg", "msk_pain_neck", "oral_other", "skin_alopecia",
                  "skin_cellulitis", "skin_decubitus", "skin_eczema", "skin_psoriasis", "skin_urticaria", "urinary_nephritis", "urinary_urolithiasis", "cvd_hf")


if(cause %in% no_asymptomatic){
  print("No asymptomatic sequelae")
  bootstrapped_results<-fread(paste0(j, "FILEPATH", cause, ".csv"))
  for(d in c(0:999)){
    sev_0_count <- length(which(bootstrapped_results[,get(paste0("DW_diff_data_", d))]<0))
    sev_0 <- sev_0_count/length(which(!is.na(bootstrapped_results[,get(paste0("DW_diff_data_", d))])))
    data[yld_cause==cause & severity==0, paste0("dist", d):=as.numeric(sev_0)]

    sev_1_count <- length(which(bootstrapped_results[,get(paste0("DW_diff_data_", d))]>=(as.numeric(data[yld_cause==cause & severity==1, paste0("MID", d, "a"), with=FALSE])) &
                                  bootstrapped_results[,get(paste0("DW_diff_data_", d))]<(as.numeric(data[yld_cause==cause & severity==1, paste0("MID", d, "b"), with=FALSE]))))
    sev_1 <- sev_1_count / (length(which(!is.na(bootstrapped_results[,get(paste0("DW_diff_data_", d))])))-sev_0_count)
    data[yld_cause==cause & severity==1, paste0("dist", d):=as.numeric(sev_1)]

    sev_2_count <- length(which(bootstrapped_results[,get(paste0("DW_diff_data_", d))]>=(as.numeric(data[yld_cause==cause & severity==2, paste0("MID", d, "a"), with=FALSE])) &
                                  bootstrapped_results[,get(paste0("DW_diff_data_", d))]<(as.numeric(data[yld_cause==cause & severity==2, paste0("MID", d, "b"), with=FALSE]))))
    sev_2 <- sev_2_count / (length(which(!is.na(bootstrapped_results[,get(paste0("DW_diff_data_", d))])))-sev_0_count)
    data[yld_cause==cause & severity==2, paste0("dist", d):=as.numeric(sev_2)]

    sev_3_count <- length(which(bootstrapped_results[,get(paste0("DW_diff_data_", d))]>=(as.numeric(data[yld_cause==cause & severity==3, paste0("MID", d, "a"), with=FALSE])) &
                                  bootstrapped_results[,get(paste0("DW_diff_data_", d))]<(as.numeric(data[yld_cause==cause & severity==3, paste0("MID", d, "b"), with=FALSE]))))
    sev_3 <- sev_3_count / (length(which(!is.na(bootstrapped_results[,get(paste0("DW_diff_data_", d))])))-sev_0_count)
    data[yld_cause==cause & severity==3, paste0("dist", d):=as.numeric(sev_3)]

    sev_4_count <- length(which(bootstrapped_results[,get(paste0("DW_diff_data_", d))]>=(as.numeric(data[yld_cause==cause & severity==4, paste0("MID", d, "a"), with=FALSE])) &
                                  bootstrapped_results[,get(paste0("DW_diff_data_", d))]<(as.numeric(data[yld_cause==cause & severity==4, paste0("MID", d, "b"), with=FALSE]))))
    sev_4 <- sev_4_count / (length(which(!is.na(bootstrapped_results[,get(paste0("DW_diff_data_", d))])))-sev_0_count)
    data[yld_cause==cause & severity==4, paste0("dist", d):=as.numeric(sev_4)]

    sev_5_count <- length(which(bootstrapped_results[,get(paste0("DW_diff_data_", d))]>=(as.numeric(data[yld_cause==cause & severity==5, paste0("MID", d, "a"), with=FALSE])) &
                                  bootstrapped_results[,get(paste0("DW_diff_data_", d))]<(as.numeric(data[yld_cause==cause & severity==5, paste0("MID", d, "b"), with=FALSE]))))
    sev_5 <- sev_5_count / (length(which(!is.na(bootstrapped_results[,get(paste0("DW_diff_data_", d))])))-sev_0_count)
    data[yld_cause==cause & severity==5, paste0("dist", d):=as.numeric(sev_5)]

    sev_6_count <- length(which(bootstrapped_results[,get(paste0("DW_diff_data_", d))]>=(as.numeric(data[yld_cause==cause & severity==6, paste0("MID", d, "a"), with=FALSE])) &
                                  bootstrapped_results[,get(paste0("DW_diff_data_", d))]<(as.numeric(data[yld_cause==cause & severity==6, paste0("MID", d, "b"), with=FALSE]))))
    sev_6 <- sev_6_count / (length(which(!is.na(bootstrapped_results[,get(paste0("DW_diff_data_", d))])))-sev_0_count)
    data[yld_cause==cause & severity==6, paste0("dist", d):=as.numeric(sev_6)]
    print(paste0("Finished disorder ", cause, ", draw ", d, " out of 999"))
  }
  data<-data[yld_cause==cause & severity!=0,]
} else {
  bootstrapped_results<-fread(paste0(j, "FILEPATH", cause, ".csv"))
  for(d in c(0:999)){
    sev_0 <- length(which(bootstrapped_results[,get(paste0("DW_diff_data_", d))]<0))/length(which(!is.na(bootstrapped_results[,get(paste0("DW_diff_data_", d))])))
    data[yld_cause==cause & severity==0, paste0("dist", d):=as.numeric(sev_0)]

    sev_1_count <- length(which(bootstrapped_results[,get(paste0("DW_diff_data_", d))]>=(as.numeric(data[yld_cause==cause & severity==1, paste0("MID", d, "a"), with=FALSE])) &
                                  bootstrapped_results[,get(paste0("DW_diff_data_", d))]<(as.numeric(data[yld_cause==cause & severity==1, paste0("MID", d, "b"), with=FALSE]))))
    sev_1 <- sev_1_count / length(which(!is.na(bootstrapped_results[,get(paste0("DW_diff_data_", d))])))
    data[yld_cause==cause & severity==1, paste0("dist", d):=as.numeric(sev_1)]

    sev_2_count <- length(which(bootstrapped_results[,get(paste0("DW_diff_data_", d))]>=(as.numeric(data[yld_cause==cause & severity==2, paste0("MID", d, "a"), with=FALSE])) &
                                  bootstrapped_results[,get(paste0("DW_diff_data_", d))]<(as.numeric(data[yld_cause==cause & severity==2, paste0("MID", d, "b"), with=FALSE]))))
    sev_2 <- sev_2_count / length(which(!is.na(bootstrapped_results[,get(paste0("DW_diff_data_", d))])))
    data[yld_cause==cause & severity==2, paste0("dist", d):=as.numeric(sev_2)]

    sev_3_count <- length(which(bootstrapped_results[,get(paste0("DW_diff_data_", d))]>=(as.numeric(data[yld_cause==cause & severity==3, paste0("MID", d, "a"), with=FALSE])) &
                                  bootstrapped_results[,get(paste0("DW_diff_data_", d))]<(as.numeric(data[yld_cause==cause & severity==3, paste0("MID", d, "b"), with=FALSE]))))
    sev_3 <- sev_3_count / length(which(!is.na(bootstrapped_results[,get(paste0("DW_diff_data_", d))])))
    data[yld_cause==cause & severity==3, paste0("dist", d):=as.numeric(sev_3)]

    sev_4_count <- length(which(bootstrapped_results[,get(paste0("DW_diff_data_", d))]>=(as.numeric(data[yld_cause==cause & severity==4, paste0("MID", d, "a"), with=FALSE])) &
                                  bootstrapped_results[,get(paste0("DW_diff_data_", d))]<(as.numeric(data[yld_cause==cause & severity==4, paste0("MID", d, "b"), with=FALSE]))))
    sev_4 <- sev_4_count / length(which(!is.na(bootstrapped_results[,get(paste0("DW_diff_data_", d))])))
    data[yld_cause==cause & severity==4, paste0("dist", d):=as.numeric(sev_4)]

    sev_5_count <- length(which(bootstrapped_results[,get(paste0("DW_diff_data_", d))]>=(as.numeric(data[yld_cause==cause & severity==5, paste0("MID", d, "a"), with=FALSE])) &
                                  bootstrapped_results[,get(paste0("DW_diff_data_", d))]<(as.numeric(data[yld_cause==cause & severity==5, paste0("MID", d, "b"), with=FALSE]))))
    sev_5 <- sev_5_count / length(which(!is.na(bootstrapped_results[,get(paste0("DW_diff_data_", d))])))
    data[yld_cause==cause & severity==5, paste0("dist", d):=as.numeric(sev_5)]

    sev_6_count <- length(which(bootstrapped_results[,get(paste0("DW_diff_data_", d))]>=(as.numeric(data[yld_cause==cause & severity==6, paste0("MID", d, "a"), with=FALSE])) &
                                  bootstrapped_results[,get(paste0("DW_diff_data_", d))]<(as.numeric(data[yld_cause==cause & severity==6, paste0("MID", d, "b"), with=FALSE]))))
    sev_6 <- sev_6_count / length(which(!is.na(bootstrapped_results[,get(paste0("DW_diff_data_", d))])))
    data[yld_cause==cause & severity==6, paste0("dist", d):=as.numeric(sev_6)]
    print(paste0("Finished disorder ", cause, ", draw ", d, " out of 999"))
  }
}

dist_names <- c(0:999)
dist_names<-paste0("dist", dist_names)
data[,`:=`(dist_mean = apply(.SD, 1, mean, na.rm=T), dist_lci = apply(.SD, 1, quantile, probs=0.025, na.rm=T), dist_uci = apply(.SD, 1, quantile, probs=0.975, na.rm=T)), by=.(hhseqid, yld_cause, grouping),.SDcols=dist_names]
data[asymp==1, `:=`(healthstate.x="asymptomatic", healthstate.y="asymptomatic", healthstate_id=-999, mean_dw=0, hhseqid=-999)]

write.csv(data[yld_cause==cause,], file=paste0(j,"FILEPATH/", cause, "FILEPATH.csv"), row.names=F)

