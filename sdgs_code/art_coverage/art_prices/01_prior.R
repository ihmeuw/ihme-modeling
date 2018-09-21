list.of.packages <- c("lme4","frontier","stringdist","gdata","sp","data.table","plyr","shiny","shinydashboard", "RMySQL","rhdf5","ggplot2","plotly","haven","parallel","quantreg","SDMTools")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only = TRUE)
jpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
cores <- 40



multiplot <- function(..., plotlist=NULL, file, cols=1, layout=NULL) {
  library(grid)
  # Make a list from the ... arguments and plotlist
  plots <- c(list(...), plotlist)
  numPlots = length(plots)
  # If layout is NULL, then use 'cols' to determine layout
  if (is.null(layout)) { 
    layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),            
                     ncol = cols, nrow = ceiling(numPlots/cols))}
  if (numPlots==1) { 
    print(plots[[1]])
  } else {
    # Set up the page
    grid.newpage()
    pushViewport(viewport(layout = grid.layout(nrow(layout), ncol(layout))))
    # Make each plot, in the correct location
    for (i in 1:numPlots) {
      # Get the i,j matrix positions of the regions that contain this subplot
      matchidx <- as.data.frame(which(layout == i, arr.ind = TRUE))
      print(plots[[i]], vp = viewport(layout.pos.row = matchidx$row,
                                      
                                      layout.pos.col = matchidx$col))
    }
    
  }
  
}



##################################################################
#Prep Data
####################################################################
#Read in Data
raw.transactions <- data.frame(fread(paste0(jpath,"FILEPATH/raw_data.csv")))
trans.sub <- raw.transactions[,c(1,2,3,7,20,24)]
names(trans.sub) <- c("location_name","data.source","drug","doses","annual_cost","year")
trans.sub <- data.table(trans.sub)
trans.sub[,doses:=as.numeric(doses)]
trans.sub[,annual_cost:=as.numeric(annual_cost)]
trans.sub[,year:=as.integer(year)]

#Identify Non-Matching Locations to GBD
all.locs <- data.table(read_dta(paste0(jpath,"FILEPATHIHME_GDB_2015_LOCS_6.1.15.dta")))
all.locs <- all.locs[location_type_id %in% c(2,8)]
all.locs<- all.locs[,.SD,.SDcols=c("ihme_loc_id","location_name","region_name","super_region_name","location_id")]
trans.sub <- data.table(merge(trans.sub,all.locs,by=c('location_name'),all.x=T))
non.matches <- unique(trans.sub[is.na(ihme_loc_id),location_name])
#Take best guess at match with fuzzy string matching
matches <- vector()
for (x in 1: length(non.matches)) {
case <- non.matches[x]
test.locs <- all.locs[,location_name]
dists <- stringdist(test.locs,case,method="lcs")
test.locs <- data.table(test.locs,dists)
match <-test.locs[dists==min(dists),test.locs]
matches <- rbind(matches,match)
}
map <- data.table(location_name=matches,country=non.matches)
names(map) <- c("location_name","country")

#Human Recodes
map[6,location_name:= "South Korea"]
map[8,location_name:= "Zambia"]
map[9,location_name:= "Iran"]
map[10,location_name:= "Laos"]
map[18,location_name:= "Syria"]

#Regional Data -- drop for now
map[c(11:14,19),location_name:= ""]

#Merge on new location names
trans.sub[,country:=location_name]
trans.sub <- trans.sub[,.SD,.SDcols=c("country","data.source","drug","doses","annual_cost","year")]
trans.sub <- data.table(merge(trans.sub,map,by="country",all.x=T))
trans.sub[!is.na(location_name),country:=location_name]
trans.sub[,location_name:=country]
trans.sub <- data.table(merge(trans.sub,all.locs,by=c('location_name')))

d.cost <- trans.sub[,.(cost=weighted.mean(annual_cost,doses)),by=.(location_name,year)]



##################################################################
#Frontier Analysis To find minimum values for price forecasts
####################################################################

##Prediction Template
template <- expand.grid(location_name=unique(all.locs[,location_name]),year=seq(1996,2040))
template <- data.table(merge(template,all.locs,by=c('location_name'),all.x=T))
d.cost <- data.table(merge(d.cost,template,by=c('location_name',"year"),all=T))

#Merge on covariates
ldi <- fread(paste0(jpath,"FILEPATH/ldi_forecasts_20160921.csv"))
d.cost[,year_id:=year]
d.cost <- data.table(merge(d.cost,ldi,by=c('location_id',"year_id"),all.x=T))

#transform cost to 0 to 1 scale, with 1 being minimum cost, then take logit
max_cost <-  max(d.cost[,cost],na.rm=T)
min_cost <- min(d.cost[,cost],na.rm=T)
cost_range = max_cost - min_cost
d.cost[,cost2 := gtools::logit(1 - ((cost - min_cost) / cost_range) + .00001)]

#Fit stochastic frontier analysis to fnd minimal values
front.mod <- sfa(formula=cost2~year,data=d.cost)
#Coefficients of model -- basically an OLS regression that finds frontier instead of mean
summary(front.mod)
#Make predictions (manually because function won't extrapolate)
d.cost[,logit_frontier:= (coefficients(front.mod)[1] + (coefficients(front.mod)[2]  * year))]
#Plot frontier predictions in original space
#Transform to actual cost
d.cost[,frontier:= ((1 - gtools::inv.logit(logit_frontier)) * cost_range) + min_cost]

#Illustrate Frontier Predictions
gg1 <- ggplot(d.cost) + geom_point(aes(y=cost2,x=cost))  + labs(y="Logit Transformed Cost",title="Cost Transformation")
gg3 <- ggplot(d.cost) + geom_point(aes(y=cost2,x=year)) + geom_line(aes(y=logit_frontier,x=year)) + labs(y="Transformed Cost",title="Frontier Predictions")
gg2 <- ggplot(d.cost) + geom_point(aes(y=cost,x=year)) + geom_line(aes(y=frontier,x=year)) + labs(title="Transformed Frontier")
fronts <- d.cost[,.(frontier=mean(frontier)),by=.(year)]
gg4 <- ggplot(fronts) + geom_bar(aes(y=frontier,x=year),stat='identity') + labs(y="Cost Frontier",title="Cost Frontier By Year")
multiplot(gg1,gg2,gg3,gg4,cols=2)

##################################################################
#Make Linear Model Predictions
####################################################################

#Transform using frontier
d.cost[cost <= frontier,cost:= frontier + 1]
d.cost[,ln_cost:=log(cost-frontier)]
d.cost[,ln_ldi:=log(ldi)]

#linear model - nest regional in super-regional slopes
prior.mod <- lm(ln_cost~ln_ldi+year_id:super_region_name,data=d.cost)

summary(prior.mod)
d.cost[,ln_prior:=predict(prior.mod,newdata=d.cost)]
d.cost[,prior:=exp(ln_prior)+frontier]

#Make up standard error for data points
uncert <- d.cost[,.(cost_se = sd(ln_cost,na.rm=T)),by=.(super_region_name)]
d.cost <- data.table(merge(d.cost,uncert,by="super_region_name",all.x=T))

#Save Predictions
write.csv(d.cost,file=paste0(jpath,"FILEPATH/prior.csv"))