## GRAB SURVEY SPECIFIC DATA AND INTERPOLATE RESULTS FROM SF-12 TO DW SPACE USING CROSSWALK SURVEY

library(foreign)
library(mgcv)

data <- fread("FILEPATH.csv")

## sort
data <- data[order(data$sf),]

## set model
model <- loess(dw ~ sf, data=data, span=.88, control=loess.control(surface=c("direct")))

plot(model$x,model$y)
lines(model$x,model$fitted)

## fit model to prediction
dw_hat <- predict(model,newdata=data.frame(sf=data$predict))

plot(data$predict,dw_hat, xlim = c(40,130))
points(data$sf,data$dw,col="red")

## outsheet it
write.csv(data.frame(data,dw_hat),"FILEPATH.csv", row.names=FALSE)

