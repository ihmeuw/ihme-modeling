
#Generate CFR by age/sex for all Hep sub-types
library(data.table)
library(plyr)
library(ggplot2)
library(Hmisc)

df <- fread("")
df2 <- fread("")

df <- rbind(df, df2)

heps <- c("a", "b", "c", "e")

for (hep in heps){
  print(hep)
  df[grep(paste0("hepatitis ", hep), Type), Type:=paste0("hep_", hep)]
}

df <- df[source=="Brazil",]

df[, `:=`(deaths_agg = sum(.SD$Deaths),
          admit_agg  = sum(.SD$Admissions)),
          by=c("Age", "Type", "source")]

df[, cfr:=deaths_agg/admit_agg]
df <- df[cfr!=0]

df <- unique(df[, .(Age, Type, source, cfr)])
setorder(df, Type, source, Age)

ideal <- expand.grid(Age=unique(df$Age), Type=unique(df$Type))
ideal$source <- "Brazil"

df <- data.table(join(df, ideal, by=c("Age", "Type", "source"), type="right"))

df[, cfr_smooth := predict(loess(cfr~Age, data=.SD, na.action=na.exclude, span=0.8, degree=2)), by=c("Type")]
df[, cfr_smooth := approxExtrap(.SD$Age, .SD$cfr_smooth, xout=unique(.SD$Age), method = "linear", na.rm=T)$y, by=c("Type")]

ggplot(df, aes(x=Age, y=cfr_smooth, color=Type)) + 
  geom_line(size=1) + geom_point(aes(y=cfr), alpha=0.4) + 
  ggtitle("CFR by Hep subtype") + 
  ylim(lims=c(0,0.25)) + 
  labs(x="Age", y="CFR", color="Hepatitis subtype") + 
  theme(axis.title.y = element_text(angle=0, vjust=0.5), plot.title = element_text(hjust=0.5)) + 
  scale_color_discrete(labels = c("Hepatitis A", "Hepatitis B", "Hepatitis C", "Hepatitis E"))

#For each Hep subtype, take mean level as given from published study and adjust curves, after removing potential age-trends (weighted pop average of CFR sums to observed CFR all-ages)

source("")
source("")

cfr_a <- mean(rbeta(10000, 3.696613, 15202.12))
cfr_b <- mean(rbeta(10000, 58, 12164))
cfr_c <- mean(rbeta(10000, 3, 2445))

#For E, study reports probability of death given symptomatic & probability of symptomatic given infection
cfr_e <- 0.019 * 0.198

#For b & c, de-pop structure the estimates

#Get population structure and convert each to a percent of total population
pop <- get_population(age_group_id = -1, sex_id=-1, location_id=86, year_id=-1)
pop <- pop[(sex_id==3 & year_id==1990),]

#Only hold onto age groups between 0-99
pop <- pop[age_group_id >=5 & !age_group_id %in% c(21:26),]

pop[, percent := population/sum(population)]

#Rename age groups to match CFR and clean pop datatable for merging
ages <- get_ids("age_group")
pop <- join(pop, ages, by="age_group_id", type="left")

pop[age_group_name=="<1 year", age_group_name:=0]
pop[, age_group_name := gsub(" .*", "", age_group_name, perl=T)]
setnames(pop, "age_group_name", "Age")
pop <- pop[, .(Age, age_group_id, percent)]

#Trying to find a constant such that population weighted average CFR * constant = Observed all age CFR
#(e.g. Weighted CFR = constant * smoothed CFR)

cfr_scale_pop_weight <- function(alphabet){
  
  cfr <- get(paste0("cfr_", alphabet))
  temp  <- df[Type==paste0("hep_", alphabet)]
  
  temp <- data.table(join(temp, pop, by="Age", type="left"))
  temp[, weight_share:=cfr_smooth*percent]
  weight_avg <- sum(temp$weight_share)
  constant <- cfr/weight_avg
  temp[, cfr_scaled := cfr_smooth * constant]
  return(temp)
  
}

#For A & E, don't weight, just find the scaling constant

cfr_scale <- function(alphabet){
  
  cfr <- get(paste0("cfr_", alphabet))
  temp  <- df[Type==paste0("hep_", alphabet)]
  
  temp <- data.table(join(temp, pop, by="Age", type="left"))
  temp[, weight_percent :=cfr_smooth/sum(cfr_smooth)]
  temp[, weight_share := cfr_smooth * weight_percent]
  weight_avg <- sum(temp$weight_share)
  
  constant <- cfr/weight_avg
  temp[, cfr_scaled := cfr_smooth * constant]

  return(temp)
  
}

a <- cfr_scale("a")
b <- cfr_scale_pop_weight("b")
c <- cfr_scale_pop_weight("c")
e <- cfr_scale("e")
  
final <- data.table(rbind(a,b,c,e, fill=T))

ggplot(final, aes(x=Age, y=cfr_scaled, color=Type)) + 
  geom_line(size=1) +
  ggtitle("CFR rescaled to all-age CFR") +
  labs(x="Age", y="CFR \nRescaled", color="Hepatitis subtype") + 
  theme(axis.title.y = element_text(angle=0, vjust=0.5), plot.title = element_text(hjust=0.5)) + 
  scale_color_discrete(labels = c("Hepatitis A", "Hepatitis B", "Hepatitis C", "Hepatitis E"))

#Add on correct age_group_ids & cause_ids,and export.
cause_id     <- c(401, 402, 403, 404)
age_group_id <- c(seq(2,20), seq(30,32), 235)

template <- data.table(expand.grid(age_group_id=age_group_id, cause_id=cause_id, garbage=NA))

#Replace hep type with cause_id for merging purposes and fill
setnames(final, "Type", "cause_id")

id <- 401
for (hep in heps){
  final[cause_id==paste0("hep_", hep), cause_id:=as.character(id)]
  id <- id + 1
}

t <- join(final, template, by=c("age_group_id", "cause_id"), type="full")

#Fix baby age groups
fix <- final[age_group_id==28, .(cause_id, cfr_scaled)]
setnames(fix, "cfr_scaled", "fix")

t <- data.table(join(t, fix, by="cause_id", type="left"))
t[is.na(cfr_scaled), cfr_scaled:=fix]

final <- t[, .(cause_id, age_group_id, cfr_scaled)]
final <- final[age_group_id != 28]
setnames(final, "cfr_scaled", "cfr")
setorder(final, cause_id, age_group_id)

write.csv(final, "", row.names=F)
