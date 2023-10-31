########################################################################
### Project:  MEPS Severity Analysis
### Purpose:  Create cut points for severity distributions and save file.
###           Running on cluster missed some causes and lead to blank
###           files so I split 3b_a into 3b_a and 3b_a_b
########################################################################

library(data.table)

## set path - use all relative paths from here on

  j <- 'FILEPATH'

## get disability weights
weights <- fread(paste0(j,"FILEPATH.csv"))

## start distributions analysis
icd_map <- fread(paste0(j,"FILEPATH.csv"))
icd_map <- unique(icd_map[, .(yld_cause, grouping, healthstate, hhseqid)])
icd_map <- (icd_map[healthstate!="asymptomatic", ])

## pull 1000 draws from weights
data <- unique(merge(icd_map, weights, by = c("hhseqid")))

# get the mean weight for reporting later on
draw_names <- c(0:999)
draw_names<-paste0("draw", draw_names)
data[,`:=`(mean_dw = apply(.SD, 1, mean)),by="hhseqid",.SDcols=draw_names]

# Generate severities based on constituent healthstates
data<-data[order(yld_cause, grouping, mean_dw),]
data<-unique(data, by=c("hhseqid", "yld_cause", "grouping", "healthstate.x"))
data <- data[, severity:=rank(mean_dw), by=.(yld_cause, grouping)]

# get max number of severities for each cause
data <- data[, maxsev:=max(severity), by=.(yld_cause, grouping)]

# for causes with more than one severity, make a new line for asymptomatic
data <- data[,asymp:=0]
data_newrow <- data[severity==1,]
data_newrow <- data_newrow[, asymp:=1]
data <- rbind(data, data_newrow)
data <- data[asymp==1, severity := 0]

# get 1000 draws of the midpoints between draws
for(d in c(0:999)){
  data[asymp==1, paste0("MID", d, "a"):=0]
  data[asymp==1, paste0("MID", d, "b"):=0]
}

# get midpoints
for(d in c(0:999)){
  data[severity==1, paste0("MID", d, "a"):=0, by=c("yld_cause", "grouping")]
  setnames(data, paste0("draw", d), "draw_calc")
  data <- data[severity==1 | severity==2, paste0("MID", d, "b") := mean(draw_calc), by=.(yld_cause, grouping)]
  data <- data[severity==1 | severity==2, MID_calc := mean(draw_calc), by=.(yld_cause, grouping)]
  data <- data[severity==2, paste0("MID", d, "a") := MID_calc]
  data <- data[severity==2 | severity==3, paste0("MID", d, "b") := mean(draw_calc), by=.(yld_cause, grouping)]
  data <- data[severity==2 | severity==3, MID_calc := mean(draw_calc), by=.(yld_cause, grouping)]
  data <- data[severity==3, paste0("MID", d, "a") := MID_calc]
  data <- data[severity==3 | severity==4, paste0("MID", d, "b") := mean(draw_calc), by=.(yld_cause, grouping)]
  data <- data[severity==3 | severity==4, MID_calc := mean(draw_calc), by=.(yld_cause, grouping)]
  data <- data[severity==4, paste0("MID", d, "a") := MID_calc]
  data <- data[severity==4 | severity==5, paste0("MID", d, "b") := mean(draw_calc), by=.(yld_cause, grouping)]
  data <- data[severity==4 | severity==5, MID_calc := mean(draw_calc), by=.(yld_cause, grouping)]
  data <- data[severity==5, paste0("MID", d, "a") := MID_calc]
  data <- data[severity==5 | severity==6, paste0("MID", d, "b") := mean(draw_calc), by=.(yld_cause, grouping)]
  data <- data[severity==5 | severity==6, MID_calc := mean(draw_calc), by=.(yld_cause, grouping)]
  data <- data[severity==6, paste0("MID", d, "a") := MID_calc]
  data <- data[severity==6 | severity==7, paste0("MID", d, "b") := mean(draw_calc), by=.(yld_cause, grouping)]
  data <- data[severity==6 | severity==7, MID_calc := mean(draw_calc), by=.(yld_cause, grouping)]
  data <- data[severity==7, paste0("MID", d, "a") := MID_calc]
  data <- data[severity==maxsev, paste0("MID", d, "b") := 1]
  setnames(data, "draw_calc", paste0("draw", d))
  print(paste0("Finished draw ", d, " of 999"))
}

## drop 1000 draws of DW, dont need them anymore
data[,paste(draw_names):=NULL]


## we now have severity cutoffs for each condition, now for each draw of each condition,
## we have to get the dristibution out of meps.

write.csv(data, file=paste0(j, "FILEPATH.csv"), row.names=F)




