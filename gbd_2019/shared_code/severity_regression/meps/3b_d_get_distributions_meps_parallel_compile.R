########################################################################
### Project:  MEPS Severity Analysis
### Purpose:  Merges cause-specific severity proportion files created by
###           3b_b and saves as a single master file.
########################################################################

library(data.table)

  j <- 'FILEPATH'

filelist <- list.files(path=paste0(j,"FILEPATH"), pattern = "csv", full.names = TRUE)
datalist <- lapply(filelist, function(x){
  read.csv(file=x, header = TRUE)
})

data <- Reduce(function(x, y){rbind(x, y)}, datalist)
data <- data.table(data)

write.csv(data, file=paste0(j,"FILEPATH.csv"), row.names=F)

data_dist <- data[, .(yld_cause, grouping, healthstate.x, healthstate.y, hhseqid, severity, mean_dw, dist_mean, dist_lci, dist_uci)]
data_dist<-data_dist[order(yld_cause, grouping, severity),]

write.csv(data_dist, file=paste0(j,"FILEPATH.csv"), row.names=F)
