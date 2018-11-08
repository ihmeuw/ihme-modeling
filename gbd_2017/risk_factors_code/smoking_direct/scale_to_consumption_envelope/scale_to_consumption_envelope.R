###################################################################
## Purpose: Scale Amount Consumed Estimates to Supply-Side Envelope
###################################################################

# Source libraries
source('FILEPATH')

# Read the location as an argument
args <- commandArgs(trailingOnly = TRUE)
l <- ifelse(!is.na(args[1]),args[1], 4718) # take location_id

# Set some useful objects
idvars<-c("location_id", "year_id", "age_group_id", "sex_id")
drawvars<-c(paste0("draw_", 0:999))

# Step 1: Generate age-sex pattern

# Pull survey-data derived estimates of amount consumed
asp<-fread('FILEPATH')
# Expand age groups (apply 21 to 30, 31, 32, and 235)
temp<-asp[age_group_id==21]
for(i in c(30, 31, 32, 235)) {
  temp<-temp[, age_group_id:=i]
  asp<-rbind(asp, temp)
}
asp<-asp[age_group_id!=21]
  
# Pull smoking prevalence draws
prevalence<-fread('FILEPATH')
temp<-prevalence[age_group_id==21]
for(i in c(30, 31, 32, 235)) {
  temp<-temp[, age_group_id:=i]
  prevalence<-rbind(prevalence, temp)
}
prevalence<-prevalence[age_group_id!=21]

# Pull populations 
pops<-get_population(age_group_id = c(7:20,30,31,32,235), sex_id = c(1, 2), location_id = l, year_id = 1960:2017, gbd_round_id = 5, status = "best")[, run_id:=NULL]

# Merge and make a number of smokers df
num_smokers<-merge(prevalence, pops, by = idvars)
num_smokers<-num_smokers[, (drawvars):=lapply(.SD, function(x) x*population), .SDcols=drawvars]

# Now scale up to produce number of cigarettes 
asp<-melt(asp, id.vars = idvars, measure.vars = drawvars)
setnames(asp, "value", "cigperday")

num_smokers<-melt(num_smokers[, population:=NULL], id.vars = idvars, measure.vars = drawvars)
setnames(num_smokers, "value", "num_smokers")

asp<-merge(asp, num_smokers, by = c(idvars, "variable"))
asp<-asp[, cigperday:=cigperday*num_smokers]

# Finally, make the proportion
asp<-asp[, prop:=cigperday/sum(cigperday), by = c("location_id", "year_id", "variable")]

# Assume same proportion for years 1960-1979
temp<-asp[year_id==1980]
for(i in 1960:1979) {
  temp<-temp[, year_id:=i]
  asp<-rbind(asp, temp)
}

# Step 2: Split out total envelope of supply-side cig
# Read in supply side data
supply<-fread('FILEPATH')
supply<-supply[, c("age_group_id", "sex_id"):=NULL]

# Make pop 15+ to calculate total number of cigarettes consumed
pops_above15<-pops[age_group_id>=8]
pops_above15<-pops_above15[, population:=sum(population), by = c("location_id", "year_id")]
pops_above15<-unique(pops_above15[,c("age_group_id", "sex_id"):=NULL])

# Calculate total number of cigarettes
supply<-merge(supply, pops_above15, by = c("location_id", "year_id"))
supply<-supply[, (drawvars):=lapply(.SD, function(x) x*population), .SDcols=drawvars]
supply<-melt(supply, id.vars = c("location_id", "year_id"), measure.vars = drawvars)
setnames(supply, "value", "cig_envelope")

# Split out the cigarettes
out<-merge(asp, supply, by = c("location_id", "year_id", "variable"))
out<-out[, amt_consumed:=((cig_envelope*prop)/num_smokers)/365]
out<-out[, orig:=cigperday/num_smokers]

# Save summaries
summaries<-copy(out)
setnames(summaries, "variable", "draw")
summaries<-melt(summaries, id.vars = c("draw", "location_id", "year_id", "age_group_id", "sex_id"), measure.vars = c("num_smokers", "prop", "cig_envelope", "amt_consumed", "orig"))
var = 'value'
summaries <- summaries[, c("mean", "lower", "upper"):=as.list(c(mean(get(var)), quantile(get(var), c(0.025, 0.975)) )), by=c(idvars, "variable")]
summaries <-unique(summaries[,.(location_id, year_id, age_group_id, sex_id, mean, lower, upper, variable)])
write.csv(summaries, paste0('FILEPATH'), na = "", row.names = F)

# Save draws
out<-out[,.(location_id, year_id, age_group_id, sex_id, amt_consumed, variable)]
out<-dcast(out, location_id + year_id + age_group_id + sex_id ~ variable, value.var = "amt_consumed")
write.csv(out, paste0('FILEPATH'), na = "", row.names = F)

