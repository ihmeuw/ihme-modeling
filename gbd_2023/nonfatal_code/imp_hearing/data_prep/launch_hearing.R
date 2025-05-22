################################################################################################
### Purpose: Launch Hearing MrBrt Script
#################################################################################################
# setup -------------------------------------------------------------------------------------------------------------------------
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
} else {
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
}
user <- Sys.info()["user"]
j_hear <- paste0(j_root,"FILEPATH", user, "FILEPATH")
hxwalk_temp <- paste0(j_root, "FILEPATH", user, "FILEPATH")
library(openxlsx)
library(logitnorm)
library(msm, lib.loc = "FILEPATH")
library(ggplot2)
repo_dir <- "FILEPATH"

source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))

source(paste0(h_root,"FILEPATH"))
draws <- paste0("draw_", 0:999)

#FOUNDATIONAL FILES------------------------------------------------------------------------------------------------------------------------------------
combo_path <- paste0(hxwalk_temp, "FILEPATH") 
combos <- data.table(read.xlsx(combo_path))
combos[ , mref := ifelse(name_alt == name_ref, 1, 0)] 
combos <- combos[mref != 1]
# Set directory where MRBRT models should be saved
out_dir <- paste0(hxwalk_temp,"FILEPATH")

# SYSTEM SETTINGS ------------------------------
# Specify usage params
threads <- 5
mem <- 15
runtime <- "02:00:00" 
shell <- 'FILEPATH'
queue <- "ADDRESS"
submit_mrbrt <- F
submit_plot <- T #this would have to be in an entirely separate script. I can just run it manually

#mod_type <- "log"
#Scripts
run_mrbrt_script <- paste0(hxwalk_temp,"FILEPATH")

#LAUNCH MRBRT JOBS FOR EACH UNIQUE ALT TO REF THRESHOLD COMBINATION IN THE COMBOS FILE------------------------------------------------------------------
for (i in 1:nrow(combos)){
  row <- i
  job_name <- paste0("hxwalk_", combos[row, name_alt], "_to_", combos[row, name_ref])
  pass_args <- list(row, out_dir, job_name, combo_path)
  construct_qsub(mem_free = mem, threads = threads, runtime = runtime, script = run_mrbrt_script, jname = job_name,
                 submit = submit_mrbrt, q = queue, pass = pass_args)
}

# READ IN RDS FILES TO PLOT DATA WITH (comment above out if sourcing)------------------------------------------------------------------
for (i in 1:nrow(combos)) {

  row <- i
  job_name <- paste0("hxwalk_", combos[row, name_alt], "_to_", combos[row, name_ref])
  hearing_crosswalk <- readRDS(paste0(out_dir, job_name,"/",job_name, "FILEPATH"))
  predictions <- readRDS(paste0(out_dir, job_name,"/predicts_",job_name, "FILEPATH"))

  model_sums <- predictions$model_summaries[1,]
  beta <- model_sums$Y_mean

  train_dt <- data.table(hearing_crosswalk$train_data)
  train_dt[sex == 1, sexn := "Male"]
  train_dt[sex == 2, sexn := "Female"]
  train_dt[age2 == 50, agen := "50+"]
  train_dt[age2 != 50, agen := paste0(age2)]
  train_dt[,study_lab:=paste0(sexn,"_",agen)]
  train_dt[w == 0, excluded := 1][w > 0, excluded := 0]

  gg <- ggplot() +
    geom_point(data = train_dt, aes(x = log_ratio, y = study_lab, color = as.factor(w))) +
    xlim(-4,4) +
    geom_errorbarh(data = train_dt, aes(y = study_lab, x = log_ratio, xmin = (log_ratio - 1.96*log_ratio_se), xmax = (log_ratio + 1.96*log_ratio_se), height=0.25, width = 0.1, color = as.factor(w))) +
    geom_vline(xintercept = beta, linetype = "dashed", color = "darkorchid") +
    geom_vline(xintercept = 0) +
    geom_rect(data = model_sums, xmin = model_sums$Y_mean_lo, xmax = model_sums$Y_mean_hi, ymin = -Inf, ymax = Inf, alpha = 0.2, fill = "darkorchid") +
    labs(x = "Logit Difference", y = "") +
    ggtitle(label = job_name) +
    theme_bw()
  print(gg)
}

dev.off()



#TRANFORM BACK FROM LOGIT SPACE & APPLY PREDS (write this into a function)-----------------------------------------------------------------------------------------------------------
#get raw data
gbdcat <- "019"
bundle_fpath <- paste0(hxwalk_temp, "FILEPATH",gbdcat, "FILEPATH")
print(bundle_fpath)
merged_extract <- data.table(read.xlsx(bundle_fpath))
nrow(merged_extract)

#fill in cases, sample size, SE
mgd_ext <- copy(merged_extract)
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

mgd_ext <- get_cases_sample_size(mgd_ext)
mgd_ext <- get_se(mgd_ext)
mgd_ext <- calculate_cases_fromse(mgd_ext)

#split hearing into each respective bundle

#apply adjustments

apply_adj <- function(){
    for (z in 1:length(sep)) {
    
    dt <- copy(mgd_ext)

    ref_thresh <- "0_19"
    print(ref_thresh)
    dt[!is.na(thresh) & (thresh != ref_thresh), alt_def := 1]
    dt[is.na(alt_def), `:=` (alt_def = 0)]
    nrow(dt[alt_def == 1])

    mgd_adjust <- dt[alt_def == 1 & mean != 0]
    nrow(mgd_adjust) 
    mgd_adjust[mean == 1, `:=` (mean = 0.9986, cases = NA)] 
    mgd_noadjust <- dt[!(alt_def %in% c(1))  | mean  == 0, ] 

    mgd_adjust[, mean_logit := logit(mean)]

    mgd_adjust[ ,mean_logit_se:=sapply(1:nrow(mgd_adjust), function(k){
      mean_i <- mgd_adjust[k, mean]
      mean_se_i <- mgd_adjust[k, standard_error]
      deltamethod(~log(x1/(1-x1)),mean_i,mean_se_i^2)
    })]

    #begin adjustments
    for (i in 1:nrow(combos)) {
      print("Checking that the right adjustment is being made...")
      if (ref_thresh == gsub("prev_","",combos$name_ref[i])) { #make sure you are adjusting alt for the correct threshold(some alts are crosswalked to multiple thresholds)

        alt_thresh <- gsub("prev_","", combos$name_alt[i])
        alt_colname <- paste0("alt_", alt_thresh)

        #create alt vars for adjustment
        print("Creating cv columns")
        mgd_adjust[ , paste0(alt_colname) := ifelse(thresh == alt_thresh, 1, 0)]

        #call in the crosswalk files
        job_name <- paste0("hxwalk_", combos[i, name_alt], "_to_", combos[i, name_ref])
        model <- readRDS(paste0(out_dir, job_name,"/",job_name, "FILEPATH"))

        preds <- predict_mr_brt(model, newdata = data.table(X_intercept = 1, Z_intercept = 1), write_draws = T)
        pred_dt <- as.data.table(preds$model_draws)
        pred_dt[, ladj := rowMeans(.SD), .SDcols = draws]
        pred_dt[, ladj_se := apply(.SD, 1, sd), .SDcols = draws]
        pred_dt[, c(draws, "Z_intercept", "X_intercept") := NULL]

        adj_vlogit <- pred_dt$ladj
        adj_vlogit_se <- pred_dt$ladj_se
        print( paste("Adjusting", alt_colname, "by logit difference ", adj_vlogit))

        mgd_adjust[get(alt_colname) == 1, `:=` (adj_logit = adj_vlogit, #calc factor to adjust by
                                             adj_logit_se = adj_vlogit_se)]

        mgd_adjust[get(alt_colname) == 1, `:=` (mean_adj_logit = mean_logit - adj_vlogit, #adjust mean in logit space
                                             mean_adj_logit_se = sqrt(mean_logit_se^2 + adj_vlogit_se^2))]

        mgd_adjust[get(alt_colname) == 1, `:=` (prev_adjusted = exp(mean_adj_logit)/(1+exp(mean_adj_logit)), #transform adjusted mean back to normal space
                                             lo_logit_adjusted = mean_adj_logit - 1.96 * mean_adj_logit_se, #calc CI in logit space
                                             hi_logit_adjusted = mean_adj_logit + 1.96 * mean_adj_logit_se)]

        mgd_adjust[get(alt_colname) == 1, `:=` (lo_adjusted = exp(lo_logit_adjusted)/(1+exp(lo_logit_adjusted)), #calc CI in normal space
                                           hi_adjusted = exp(hi_logit_adjusted)/(1+exp(hi_logit_adjusted)) )]
      }
    }
  adj_fpath <- paste0(hxwalk_temp,"FILEPATH",ref_thresh ,"FILEPATH")
  print(adj_fpath)
  write.xlsx(x=mgd_adjust, file = adj_fpath )

  #drop columns
  all_adj <- copy(mgd_adjust)
  all_adj[ , `:=` (mean = prev_adjusted, lower = lo_adjusted, upper = hi_adjusted, standard_error = mean_adj_logit_se, note_modeler = paste(note_modeler, " | adjusted with logit difference", adj_logit))]
  extra_cols <- setdiff(names(all_adj), names(mgd_noadjust))
  all_adj <- all_adj[, c(extra_cols) := NULL]
  full_dt <- rbind(all_adj, mgd_noadjust, fill = T)

  full_fpath <- paste0(hxwalk_temp,"FILEPATH",ref_thresh ,"FILEPATH")
  print(full_fpath)
  write.xlsx(x=full_dt, file = full_fpath , sheetName = "extraction")



  }
}

#USE FULL DT
full_dt <- copy(dt)
dim(full_dt)
print(unique(full_dt$note_modeler))
gbdcat <- "ID"

split_data <- function(dt, model){
  dt[ ,crosswalk_parent_seq := NA]
  tosplit_dt <- copy(dt)
  nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female") | (sex == "Both" & group_review == 0)]
  tosplit_dt <- tosplit_dt[sex == "Both" & (group_review == 1 | is.na(group_review))]
  tosplit_dt[, midyear := floor((year_start + year_end)/2)]
  preds <- predict_mr_brt(model, newdata = data.table(X_intercept = 1, Z_intercept = 1), write_draws = T) #create new data below based on x covs
  pred_draws <- as.data.table(preds$model_draws)
  pred_draws[, c("X_intercept", "Z_intercept") := NULL]
  pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]
  ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)
  ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)
  pops <- get_mort_outputs(model_name = "population single year", model_type = "estimate", demographic_metadata = T,
                           year_ids = tosplit_dt[, unique(midyear)], location_ids = tosplit_dt[, unique(location_id)], sex_ids = 1:3)
  pops[age_group_years_end == 125, age_group_years_end := 99]
  tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt, pop_dt = pops), mc.cores = 9))
  tosplit_dt <- tosplit_dt[!is.na(both_N)] ## GET RID OF DATA THAT COULDN'T FIND POPS
  tosplit_dt[, merge := 1]
  pred_draws[, merge := 1]
  split_dt <- merge(tosplit_dt, pred_draws, by = "merge", allow.cartesian = T)
  split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
  split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
  split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
  split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
  split_dt[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]
  split_dt[ , crosswalk_parent_seq := seq] 
  split_dt[!is.na(crosswalk_parent_seq), seq := NA]
  male_dt <- copy(split_dt)
  male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = "", lower = "",
                  cases = "", sample_size = "", uncertainty_type_value = "", sex = "Male",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                        ratio_se, ")"))]
  male_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
  male_dt <- dplyr::select(male_dt, names(dt))
  female_dt <- copy(split_dt)
  female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = "", lower = "",
                    cases = "", sample_size = "", uncertainty_type_value = "", sex = "Female",
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                          ratio_se, ")"))]
  female_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
  female_dt <- dplyr::select(female_dt, names(dt))
  total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt))
  return(list(final = total_dt, graph = split_dt))
}

#returns total_dt and split_dt
predict_sex <- split_data(dt = full_dt, model = hsex_model) #this function doesn't work when using a readRDS model

split <- predict_sex$graph
total <- predict_sex$final
dim(split)
dim(total)
final_dir <- paste0(hxwalk_temp, "FILEPATH")
ss_fpath <- paste0(final_dir,"sexsplit_", gbdcat ,".xlsx")
print(ss_fpath)
write.xlsx(predict_sex$final, file = ss_fpath, sheetName = "extraction")
#total_dt is sex-extended data set, cv ratios should be applied to total dt
#split_dt is all the both data points that have been split into male and female
#write.xlsx(predict_sex$graph, file = paste0(final_dir, "FILEPATH",gbdcat,"FILEPATH"), sheetName = "extraction")


#GRAPH SPLIT ------------------------------------------------------------------------------------------
graph_predictions <- function(dt){
  graph_dt <- copy(dt[measure == "prevalence", .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error)])
  graph_dt <- graph_dt[!is.na(mean),]
  graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_mean", "female_mean"))
  graph_dt_means[variable == "female_mean", variable := "Female"][variable == "male_mean", variable := "Male"]
  graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_standard_error", "female_standard_error"))
  graph_dt_error[variable == "female_standard_error", variable := "Female"][variable == "male_standard_error", variable := "Male"]
  setnames(graph_dt_error, "value", "error")
  graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable"), allow.cartesian = TRUE)
  graph_dt[, N := (mean*(1-mean)/error^2)]
  graph_dt <-graph_dt[!is.nan(N),]
  wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
  graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
  graph_dt[, midage := (age_end + age_start)/2]
  gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) +
    geom_point() +
    geom_errorbar(aes(ymin = lower, ymax = upper)) +
    labs(x = "Both Sex Mean", y = "Sex Split Means") +
    geom_abline(slope = 1, intercept = 0) +
    ggtitle(paste(gbdcat, " Sex-Split Means Compared to Both-Sex Mean")) +
    scale_color_manual(name = "Sex", values = c("Male" = "purple", "Female" = "orange")) +
    theme_classic()
  return(list(plot = gg_sex, dt = graph_dt))
}

#LEAVE IN THIS ORDER
pdf_fpath <- paste0("FILEPATH", gbdcat,"FILEPATH")
print(pdf_fpath)

pdf(pdf_fpath)
graph_predictions(dt = split)$plot
dev.off()

nrow(total[(age_end - age_start > 25) & is_outlier %in% c(0, NA) & group_review %in% c(1, NA) & year_end > 1980])
View(total[(age_end - age_start > 25) & is_outlier %in% c(0, NA) & group_review %in% c(1, NA) & year_end > 1980])

  gbd_cat <- ref_thresh

  #one plot of adj vs unadj prev
  plot_adj <- data.table(read.xlsx(paste0(hxwalk_temp,"FILEPATH", ref_thresh,"FILEPATH")))
  gg1 <- ggplot(plot_adj, aes(x=mean, y=prev_adjusted, color = as.factor(thresh))) +
    geom_point(size=1.5, alpha = 0.5) + geom_errorbar(aes(ymin = hi_adjusted, ymax = lo_adjusted)) +
    geom_abline(intercept = 0, slope = 1, color="black",linetype="dashed", size=.5) +
    labs(x="Unadjusted prevalence", y="Adjusted prevalence", title = paste0("Adjustments for GBD Category ", gbd_cat ))
  print(gg1)

  #another plot of all prevs to see relationship b/tw adj and normal (will be the same for some threshes

  plot_all <- data.table(read.xlsx(paste0(final_dir, "FILEPATH", gbdcat,"FILEPATH")))
  dim(plot_all)
  plot_all <- plot_all[is_outlier %in% c(0,NA) & group_review %in% c(1, NA)]
  dim(plot_all)
  plot_all <- merge(plot_all, super_region, by = "location_id", all.x = TRUE)
  gg2 <- ggplot(plot_all, aes(x=age_start, y=mean, color = as.factor(sex))) +
    geom_point(size=1.5, alpha = 0.5) + facet_wrap(~super_region_id) +
    labs(x="age", y="final prevalence", title = paste0("All points for Category ", gbdcat, " Post Adjustments" ))


  print(gg2)
