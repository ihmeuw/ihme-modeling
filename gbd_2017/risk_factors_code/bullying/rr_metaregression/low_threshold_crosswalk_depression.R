#########################################
######



## Load data
bully_data <- as.data.table(read.xlsx("FILEPATH", sheet="Depressive disorders extraction"))
bully_data <- bully_data[Low.or.high!=".", .(author=paste(First.Author, Publication.year), Adjusted,
                                               sex=Sex, age_start=Mean.age.at.intake, age_end=Mean.age.at.follow.up, time=Mean.years.follow.up,
                                               parameter=Parameter.type, value=Parameter.value, lower=Lower.UI, upper=Upper.UI, sd=Standard.deviation,
                                               p=p.value, exposed_cases=Exposed.cases, exposed_noncases=`Exposed.non-cases`, nonexposed_cases=`Non-exposed.cases`,
                                               nonexposed_noncases=`Non-exposed.non-cases`, exposed_sample_size=`Sample.size.(exposed)`,
                                               nonexposed_sample_size=`Sample.size.(non-exposed)`, parameter_sample_size=`Sample.size.(exposed.+.non-exposed)`,
                                               study_sample_size=`Study.sample.size.(total.N)`, cv_symptoms, cv_low_threshold_bullying, cv_child_baseline, cv_retrospective,
                                             threshold_pairs = Pairs.of.low.vs.high.threshold.bullying, Low.or.high, threshold_most_adjusted = Most.adjusted.pair.of.low.vs.high.threshold.bullying)]

bully_data[is.na(exposed_sample_size), exposed_sample_size:=exposed_cases+exposed_noncases]
bully_data[is.na(nonexposed_sample_size), nonexposed_sample_size:=nonexposed_cases+nonexposed_noncases]
bully_data[is.na(parameter_sample_size), parameter_sample_size:=exposed_sample_size+nonexposed_sample_size]
bully_data[, `:=` (cases_total=exposed_cases+nonexposed_cases, exposed_proportion = exposed_sample_size/parameter_sample_size)]


bully_data[, `:=` (arr = or_2_rr(value, cases_total/parameter_sample_size, exposed_sample_size/parameter_sample_size),
                   arr_low = or_2_rr(lower, cases_total/parameter_sample_size, exposed_sample_size/parameter_sample_size),
                   arr_high = or_2_rr(upper, cases_total/parameter_sample_size, exposed_sample_size/parameter_sample_size)), by=c("author", "value")]

bully_data[, arr_se := (log(arr_high) - log(arr_low)) / (qnorm(0.975,0, 1)*2)]
bully_data[author=="Schoon 1997", `:=` (arr = value, arr_low=lower, arr_high = upper,  arr_se = sd)]
bully_data[author=="Schoon 1997", `:=` (arr = value, arr_se = sd)]
bully_data[is.na(arr), `:=` (missing_rr=1, arr=value)]
bully_data[is.na(arr_se), arr_se := ifelse(is.na(sd), (log(upper) - log(lower))/(qnorm(0.975, 0, 1)*2), sd)]

## create ratios per pair
for(i in unique(bully_data$threshold_pairs)){
  arr_lowthres <- bully_data[threshold_pairs==i & Low.or.high=="L", arr]
  arr_lowthres_se <- bully_data[threshold_pairs==i & Low.or.high=="L", arr_se]
  bully_data[threshold_pairs==i & Low.or.high=="H", `:=` (ratio = arr / arr_lowthres,
                                                          ratio_se = sqrt(((arr^2)/(arr_lowthres^2)) *  ((arr_se^2)/(arr^2) + (arr_lowthres_se^2)/(arr_lowthres^2))))]
}
bully_data[, `:=` (yi = log(ratio), vi = ratio_se^2)]


bully_total <- rbind(bully_data, bully_data_anx)


meta_rr <- rma(measure="RR", yi=yi, vi=vi, slab=author, data=bully_total, method="DL")

crosswalk <- exp(meta_rr$beta[1])
crosswalk_se <- (exp(meta_rr$ci.ub) - exp(meta_rr$ci.lb))/(qnorm(0.975,0, 1)*2)
# Crosswalk     = 1.469779
# Crosswalk se  = 0.0966926



