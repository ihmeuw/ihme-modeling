# Check mask use output

test <- read.csv('FILEPATH/mask_use.csv')
test <- read.csv('FILEPATH/mask_use_best.csv')
test <- read.csv('FILEPATH/mask_use_worse.csv')
which(duplicated(test[,c('date','location_id')]))



