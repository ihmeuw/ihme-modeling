#Calculate percentage unrecorded 

library(data.table)
library(plyr)

recorded    <- fread("")
unrecorded  <- fread("")

#Massage unrecorded data to the proper shape
unrecorded[, mean  := as.numeric(gsub("\\[.*", "", unrecorded_lpc))]
unrecorded[, lower := as.numeric(gsub(".*\\[|\\-.*|\\]", "", unrecorded_lpc))]
unrecorded[, upper := as.numeric(gsub(".*\\[|.*\\-|\\]", "", unrecorded_lpc))]

unrecorded[, unrecorded_lpc:=NULL]

#Massage recorded data to the proper shape
recorded <- recorded[type=="All types",]
recorded[, type:=NULL]

unrecorded <- data.table(join(recorded, unrecorded, by="location", type="inner"))
unrecorded[, recorded_lpc := as.numeric(recorded_lpc)]
unrecorded <- unrecorded[(recorded_lpc!=0 & mean!=0),]

unrecorded[, unrecorded_rate_mean := (recorded_lpc + mean)/recorded_lpc]
unrecorded[, unrecorded_rate_lower := (recorded_lpc + lower)/recorded_lpc]
unrecorded[, unrecorded_rate_upper := (recorded_lpc + upper)/recorded_lpc]

unrecorded <- unrecorded[unrecorded_rate_upper <= 4,]
fwrite(unrecorded, "")
