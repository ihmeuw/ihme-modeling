# Crosswalk SF12 to GBD DW with Lowess Regession

library(foreign)
library(mgcv)

data <- read.dta("FILEPATH.dta")
data <- data[order(data[,"sf"]),]
model <- loess(dw ~ sf, data=data, span=.88, control=loess.control(surface=c("direct")))
dw_hat <- predict(model,newdata=data.frame(sf=data[,"predict"]))

write.dta(data.frame(data,dw_hat),file="FILEPATH",convert.factors="string")

