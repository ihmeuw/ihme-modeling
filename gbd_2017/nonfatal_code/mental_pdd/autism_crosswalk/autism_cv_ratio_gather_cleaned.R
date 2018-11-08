rm(list=ls())
library(data.table)
library(openxlsx)
library(metafor)
rlogit <- function(x){exp(x)/(1+exp(x))}
logit <- function(x){log(x/(1-x))}
'%!in%' <- function(x,y)!('%in%'(x,y))

data_survey <- as.data.table(read.xlsx(paste0("FILEPATH"), sheet="extraction"))[`Population-representative.survey` %in% c(0:1),]
data_registry <- as.data.table(read.xlsx(paste0("FILEPATH"), sheet="registry_extraction"))[`Population-representative.survey` %in% c(0:1),]
data_registry[,`:=`(cv_registry=NULL, cv_surveillance=NULL, cv_clinic=NULL, for_dismod=NULL, duplicate_of_nid_paper=NULL, duplicate_of_nid_dismod=NULL, fifty_percent_overlap=NULL)]
meta_data_prev <- rbind(data_survey[measure=="prevalence",], data_registry[measure=="prevalence",])

meta_data_prev[,`:=`(age_start = as.numeric(age_start), age_end = as.numeric(age_end), cases=as.numeric(cases), sample_size=as.numeric(sample_size), mean=as.numeric(mean), standard_error=as.numeric(standard_error), lower=as.numeric(lower), upper=as.numeric(upper))]
meta_data_prev[is.na(standard_error), `:=` (standard_error = ifelse(!is.na(lower), (upper - lower)/(qnorm(0.975,0, 1)*2), sqrt(1/sample_size*mean*(1-mean)+1/(4*sample_size^2)*1.96^2)))]
meta_data_prev[is.na(cases), cases := mean * sample_size]

meta_data_prev[modelable_entity_name=="Autism" & cv_autism == 0, cv_autism := 1]
meta_data_prev[modelable_entity_name=="Autism spectrum disorders" & cv_autism == 1, cv_autism := 0]

## Exclude non-DSM/ICD/CCMD criteria
meta_data_prev <- meta_data_prev[!(nid %in% c(327739, 330059, 331707, 304701, 332562, 332546, 311619)) & !(nid==126133 & case_definition=="Kanners"),]

## Check all sex-specific studies have both-sex estimates available
sex_table <- data.table(table(meta_data_prev[,nid], meta_data_prev[,sex]))
sex_table[V2=="Male" & (V1 %!in% sex_table[V2=="Both" & N>0, V1]), ] # nid 126131 contains sex specific data for nid 126130
sex_table[V2=="Female" & (V1 %!in% sex_table[V2=="Both" & N>0, V1]), ] # nid 126131 contains sex specific data for nid 126130

## Mark studies with widest age groups
meta_data_prev[,`:=` (study_max_age=max(age_end), study_min_age=min(age_start)), by="nid"]
meta_data_prev[, widest_age := ifelse(age_end == study_max_age & age_start == study_min_age,  1, 0)]
unique(meta_data_prev[widest_age == 0 & (nid %!in% meta_data_prev[widest_age==1, nid]),nid])
meta_data_prev[nid %in% c(271444, 333077), widest_age := 1] # nids 271444 and 333077 report two separate cohort estimates
meta_data_prev_329249 <- meta_data_prev[nid == 329249, ] # nid 329249 reports multiple age-specific estimates that need to be combined
meta_data_prev_329249[, `:=` (cases = sum(cases), sample_size = sum(sample_size)), by="modelable_entity_name"]
meta_data_prev_329249 <- unique(meta_data_prev_329249[,`:=` (mean=cases/sample_size, lower=NA, upper=NA, age_start=min(age_start), age_end=max(age_end), widest_age=1, year_start=min(year_start), year_end=max(year_end))])
meta_data_prev <- rbind(meta_data_prev, meta_data_prev_329249)
meta_data_prev <- meta_data_prev[widest_age==1,]

## Mark studies with non-aggregated locations
# by site memo
location_count <- data.table(table(meta_data_prev[widest_age==1 & sex == "Both",nid], meta_data_prev[widest_age==1 & sex == "Both",site_memo]))
location_count <- location_count[N>0,]
location_count <- data.table(table(location_count$V1))[N>1]
meta_data_prev[,exclude := 0]
meta_data_prev[nid == 273276 & location_id != "4625", exclude:=1]
meta_data_prev[nid == 311615 & location_id != "4853", exclude:=1]
meta_data_prev[nid == 329542 & site_memo == "Australia", exclude:=1] # National estimate by me but based on text, may be innappropriately weighted. 
# by location_id
location_count <- data.table(table(meta_data_prev[widest_age==1 & sex == "Both",nid], meta_data_prev[widest_age==1 & sex == "Both",location_id]))
location_count <- location_count[N>0,]
location_count <- data.table(table(location_count$V1))[N>1]
meta_data_prev[nid == 126100 & location_id != "90", exclude:=1] # Get rid of estimates I've location split
meta_data_prev <- meta_data_prev[exclude==0, ]

## Retain "comprehensive" estimates
meta_data_prev[, `:=` (cv_non_comprehensive=0, cv_comprehensive=0)]
setnames(meta_data_prev, "Population-representative.survey", "cv_survey")
setnames(meta_data_prev, "Population-representative.survey.+.special.population.survey/review.clinical.records", "cv_survey_plus")
setnames(meta_data_prev, "Catchment.area/representative.clinics.uses.population.screening.and.case.found.from.research.or.diagnosed.by.author.", "cv_pop_screen")
setnames(meta_data_prev, "Review.clinical./.education.records.and.determine.number.diagnosed", "cv_registry")
setnames(meta_data_prev, "Review.clinical./.education.records.to.detect.high-risk.populations.and.invite.for.clinical.evaluation", "cv_registry_eval")
setnames(meta_data_prev, "Number.of.cases.merely.nominated.by.teachers.or.clinicians.(e.g.,.via.letter)", "cv_letter")
setnames(meta_data_prev, "Cases.put.forward.by.teachers.or.clinicians.and.then.clinically.evaluated", "cv_letter_eval")
setnames(meta_data_prev, "Clinicians.review.notes.from.multiple.data.sources.records.(e.g.,.clinical./.education.records).to.determine.an.independent.diagnosis.based.on.these.notes", "cv_surveillance")
setnames(meta_data_prev, "Conduct.active.case-finding.involving.multiple.sources.of.recruitment.(e.g.,.screening.education.records,.contacting.clinics,.and.contacting.local.ASD.communities).", "cv_active")
meta_data_prev[cv_letter_eval==1, cv_letter := 1] # Note only 2 prevalence studies on cv_letter anyway
meta_data_prev[nid %in% c(126135, 330898), cv_letter := 1] # could be argued that 126135 and 330898 uses a similar voluntary methodology process
meta_data_prev[nid==120546, cv_registry_eval := 1] # could be argued that 120546 diagnoses cases from high-risk population screened via special education records.
meta_data_prev[cv_survey_plus==1 | cv_pop_screen==1 | cv_registry_eval==1, cv_comprehensive := 1]
meta_data_prev[cv_survey==1 | cv_registry==1, cv_non_comprehensive := 1]

meta_data_prev <- meta_data_prev[cv_comprehensive == 1 & cv_old_dx_criteria==0,]

## Flag studies that have both ASD and autism estimates 
asd_nids <- unique(meta_data_prev[modelable_entity_name == "Autism spectrum disorders", nid])
autism_nids <- unique(meta_data_prev[modelable_entity_name == "Autism", nid])
both_nids <- asd_nids[asd_nids %in% autism_nids]

meta_data_prev <- meta_data_prev[nid %in% both_nids,]

## Remove year-specific estimates from studies and retain all-year estimates
meta_data_prev <- meta_data_prev[!(nid==314711 & !(year_start==1988 & year_end==2002)),]

## Create paired dataset
meta_data_prev_asd <- meta_data_prev[modelable_entity_name == "Autism spectrum disorders",]
meta_data_prev_asd <- meta_data_prev_asd[, .(cases_asd = cases, sample_size_asd = sample_size, nid, sex, age_start, age_end, year_start, year_end, location_id, site_memo, m1i=mean, sd1i=standard_error, n1i=1)]
meta_data_prev_autism <- meta_data_prev[modelable_entity_name == "Autism",]
meta_data_prev_autism <- meta_data_prev_autism[, .(cases_autism = cases, sample_size_autism = sample_size, nid, sex, age_start, age_end, year_start, year_end, location_id, site_memo, m2i=mean, sd2i=standard_error, n2i=1)]

meta_data_prev_both <- merge(meta_data_prev_asd, meta_data_prev_autism, by=c("sex", "nid", "age_start", "age_end", "year_start", "year_end", "location_id", "site_memo"))
meta_data_prev_both <- meta_data_prev_both[sex=="Both",] # restrict to both sex estimates
meta_data_prev_both[nid == 120545, cases_autism := cases_asd * (m2i / m1i)] # Due to the sampling procedure and weighting, the prevalence does not match the case proportions. 

meta_data_prev_both[, ratio := m1i / m2i]
meta_data_prev_both[, prop_m := m2i / m1i]
meta_data_prev_both[, prop_c := cases_autism / cases_asd]

## Meta analysis
meta_proportion <- rma(measure="PLO", xi=cases_autism, ni=cases_asd, slab=nid, data=meta_data_prev_both, method="DL")
results <- predict(meta_proportion, digits=6, transf=transf.ilogit)

proportion <- data.table(value = results$pred, lower = results$ci.lb, upper = results$ci.ub, se = (results$ci.ub - results$ci.lb)/(2*qnorm(0.975,0, 1)))
crosswalk <- data.table(value = 1/results$pred, se = (1/results$ci.lb - 1/results$ci.ub)/(2*qnorm(0.975,0, 1)))

#######################################
###### Apply crosswalk to dataset #######
#######################################

dataset <- as.data.table(read.xlsx("FILEPATH"), sheet="extraction"))
untouched <-  dataset[measure!="prevalence" | group_review != 1,]
dataset <- dataset[measure=="prevalence" & group_review == 1,]
## Flag studies that have both ASD and autism estimates 
asd_nids <- unique(dataset[cv_autism == 0, nid])
autism_nids <- unique(dataset[cv_autism == 1, nid])
both_nids <- asd_nids[asd_nids %in% autism_nids]

dataset[(nid %in% both_nids) & cv_autism == 1, `:=` (group_review = 0, note_modeler = paste0(note_modeler, " Group_reviewed 0 in presence of ASD-overall estimate."))]
new_rows <- dataset[!(nid %in% both_nids) & cv_autism == 1, ]
dataset[!(nid %in% both_nids) & cv_autism == 1, `:=` (specificity = paste0(specificity, ", raw"), group_review = 0, note_modeler = paste0(note_modeler, " Group_reviewed 0 as autism-to-ASD crosswalk applied in separate row."))]

new_rows[, `:=` (seq = NA, lower=NA, upper=NA, mean = mean * crosswalk$value, standard_error = sqrt((standard_error^2)*(crosswalk$se^2)+(mean^2)*(crosswalk$se^2)+(crosswalk$value^2)*(standard_error^2)))]
new_rows[, `:=` (uncertainty_type = "Standard error", uncertainty_type_value = NA, cv_autism = 0, specificity = paste0(specificity, ", crosswalked"), cases = sample_size*mean, note_modeler = paste0(note_modeler, " Autism-to-ASD crosswalk of ", round(crosswalk$value, 3), " (se = ", round(crosswalk$se, 3), ") applied."))]

dataset <- rbind(untouched, dataset, new_rows)

write.csv(dataset, paste0("FILEPATH"), row.names=F, na="")

