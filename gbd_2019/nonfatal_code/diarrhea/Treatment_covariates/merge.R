os <- .Platform$OS.type
if (os == "windows") {
  source("FILEPATH")
} else {
  source("FILEPATH")
}

source(fix_path("FILEPATH"))
source(fix_path("FILEPATH"))

ors2017 <- read.csv(fix_path("FILEPATH"))
ors1 <- as.data.frame(model_load(138131,"data"))
ors2 <- as.data.frame(model_load(138152,"data"))
ors_missing_nids <- unique(ors1$nid[which(ors1$nid %not in% ors2$nid)])
ors_2017_missing <- ors_missing_nids[which(ors_missing_nids %in% ors2017$nid)]

sub_2017 <- subset(ors2017,nid %in% ors_2017_missing)
setnames(sub_2017,"val","data")
setnames(sub_2017,"cv_outlier","is_outlier")
sub_2017 <- sub_2017[,which(names(sub_2017) %in% names(ors2))]
for(col in 1:ncol(ors2)){
  ors2[,col] <- as.character(ors2[,col])
}
for(col in 1:ncol(sub_2017)){
  sub_2017[,col] <- as.character(sub_2017[,col])
}

combo <- rbind(ors2,sub_2017)
write.csv(combo,"FILEPATH")
