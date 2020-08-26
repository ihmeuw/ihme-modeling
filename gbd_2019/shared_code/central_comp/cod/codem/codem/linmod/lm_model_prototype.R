rm(list=ls())

.libPaths("FILEPATH")
library(data.table); library(lme4); library(parallel); library(gdata)
library(jsonlite, lib.loc='FILEPATH')

args <- commandArgs(TRUE)
model_version_id <- args[1]
model_dir <- args[2]
DB <- args[3]
cores_N <- args[4]
setwd(model_dir)

output_file <- file("FILEPATH", open="wt")
sink(output_file)
sink(output_file, type="message")


options(lmerControl=list(check.nobs.vs.rankZ = "warning",
                         check.nobs.vs.nlev = "warning",
                         check.nobs.vs.nRE = "warning",
                         check.nlev.gtreq.5 = "warning",
                         check.nlev.gtr.1 = "warning"))

print(paste0("Running linear models for model version id: ",
             rev(strsplit(getwd(), "/")[[1]])[1]))
date()

rate_vars <- fromJSON("step_2/cv_selected_ln_rate.txt")[["ln_rate_vars"]]
cf_vars <- fromJSON("step_2/cv_selected_lt_cf.txt")[["lt_cf_vars"]]
if (class(rate_vars) == "matrix"){
    if(dim(rate_vars)[1] == 1){
        rate_vars <- list(as.vector(rate_vars))
    } else {
        rate_vars <- t(rate_vars)
        rate_vars <- split(rate_vars, rep(1:ncol(rate_vars), each=nrow(rate_vars)))
        names(rate_vars) <- NULL
    }
}
if (class(cf_vars) == "matrix"){
    if(dim(cf_vars)[1] == 1){
        cf_vars <- list(as.vector(cf_vars))
    } else {
        cf_vars <- t(cf_vars)
        cf_vars <- split(cf_vars, rep(1:ncol(cf_vars), each=nrow(cf_vars)))
        names(cf_vars) <- NULL
    }
}

df <- fread("inputs/input_database_square.csv")
df$age <- as.factor(df$age)
df$has_sub_nat <- as.numeric(df$location_id != df$country_id)
df$super_region <- as.character(df$super_region)

ko <- fread ("inputs/ko_data.csv")

cohort_mean <- function(subject, intercept_df){
    other <- which(startsWith(row.names(intercept_df),
                              gsub("[^:]*$", "", subject)))
    group_mean <- mean(intercept_df[other,])
    group_mean[is.na(group_mean)] <- 0
    return(group_mean)
}

new_rows <- function(names, values, df){
    df[names,] <- values
    return(df)
}

update_ran_eff <- function(ran_eff, df){
    missing_ <- lapply(names(ran_eff), function(x)
        setdiff(df[[x]], row.names(ran_eff[[x]])))
    names(missing_) <- names(ran_eff)
    update_ <- lapply(names(ran_eff), function(x)
        new_rows(names=missing_[[x]],
                 values=sapply(missing_[[x]], cohort_mean, ran_eff[[x]]),
                 df=ran_eff[[x]]))
    names(update_) <- names(ran_eff)
    return(update_)
}

paste_model_lm <- function(x, y, i){
    variables <- do.call("paste", c(as.list(x), sep=" + "))
    paste0("lmer(", y, " ~ ", variables, " + age + (1|super_region) + ",
           "(1|region_nest) + (1|age_nest) + (1|country_nest)",
           ", data=subset(df, ko[[", i, "]]), weights=weight)")
}

extract_merMod <- function(merMod, df){
    merMod_list <- list()
    merMod_list[["vcov"]] <- as.matrix(vcov(merMod))
    merMod_list[["fix_eff"]] <- data.frame(values=fixef(merMod))
    merMod_list[["ran_eff"]] <- update_ran_eff(ranef(merMod), df)
    row.names(merMod_list[["ran_eff"]][["super_region"]]) <-
        paste0("_", row.names(merMod_list[["ran_eff"]][["super_region"]]))
    return(merMod_list)
}

# wrapper in case errors come about because only one age group exists in a knockout
run_lmer_age_wrapper <- function(call){
    print(call)
    out <- tryCatch({eval(parse(text=call))},
                    error=function(cond){eval(parse(text=gsub("\\+ age ", "", call)))})
    out
}

all_kos <- function(call, df, ko){
    kos <- extract_merMod(run_lmer_age_wrapper(call), df)
    kos
}

all_calls <- function(df, ko, calls, cores=4){
    temp <- mclapply(calls, all_kos, df=df, ko=ko, mc.cores=cores)
    ko_num <- ncol(ko)/3
    starts <- seq(1,length(temp), by=ko_num)
    temp <- lapply(starts, function(x) temp[x:(x + ko_num - 1)])
    type <- sapply(calls[starts], function(x)
        gsub("lmer\\(", "", strsplit(x, " ")[[1]][1]))
    names(temp) <- paste0(type, "_model", sprintf("%03d", 1:length(temp)))
    ko_names <- paste0("ko", sprintf("%02d", 1:ko_num))
    for (i in 1:length(temp)) names(temp[[i]]) <- ko_names
    temp
}

lm_rate_calls <- as.vector(sapply(rate_vars, paste_model_lm, y="ln_rate",
                                  i = seq(1, ncol(ko), 3)))
lm_cf_calls <- as.vector(sapply(cf_vars, paste_model_lm, y="lt_cf",
                                i = seq(1, ncol(ko), 3)))

drop_country_nest <- function(models){
    object <- models
    for(model in 1:length(object)){
        for(ko in 1:length(object[[model]])){
            object[[model]][[ko]]$ran_eff$country_nest <- NULL
        }
    }
    return(object)
}

lin_mods <- all_calls(df, ko, c(lm_rate_calls, lm_cf_calls), cores=cores_N)
print("Dropping country nested random effects next...")
space_mods <- drop_country_nest(lin_mods)

print(paste0("Restructuring data to JSON for model version ID: ",
             rev(strsplit(getwd(), "/")[[1]])[1]))

write_json(x=lin_mods, path=paste0(model_dir, "/step_3/linear_model_json.txt"), dataframe="columns", digits=14)
write_json(x=space_mods, path=paste0(model_dir, "/step_3/space_time_json.txt"), dataframe="columns", digits=14)

print("Finished!!!!!")
date()

sink(type = "message")
sink()
