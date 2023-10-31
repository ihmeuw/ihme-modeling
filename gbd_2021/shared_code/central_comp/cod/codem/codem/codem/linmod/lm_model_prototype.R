# get the neccassary libraries
rm(list=ls())

library(data.table)
library(lme4)
library(parallel)
library(gdata)
library(jsonlite)
library(logging)

# set lmer options to warn instead of error to catch convergence issues
options(lmerControl=list(check.nobs.vs.rankZ = "warning",
                         check.nobs.vs.nlev = "warning",
                         check.nobs.vs.nRE = "warning",
                         check.nlev.gtreq.5 = "warning",
                         check.nlev.gtr.1 = "warning"))

# get function arguments and set constants
args <- commandArgs(TRUE)
model_version_id <- args[1]
model_dir <- args[2]
DB <- args[3]
cores_N <- args[4]
usa_re <- ifelse(as.numeric(args[5]) == 11, TRUE, FALSE)

setwd(model_dir)

# set up logging
logReset()
basicConfig(level='DEBUG')
log_file <- paste0("FILEPATH")
suppressWarnings(file.remove(log_file))
addHandler(writeToFile, file=log_file)
loginfo(paste0("Running linear model builds for model version ID ", model_version_id))

# pull selected ln_rate and lt_cf covariates
rate_vars <- fromJSON("FILEPATH")[["ln_rate_vars"]]
if (class(rate_vars) == "matrix"){
    if(dim(rate_vars)[1] == 1){
        rate_vars <- list(as.vector(rate_vars))
    } else {
        rate_vars <- t(rate_vars)
        rate_vars <- split(rate_vars, rep(1:ncol(rate_vars), each=nrow(rate_vars)))
        names(rate_vars) <- NULL
    }
}
cf_vars <- fromJSON("FILEPATH")[["lt_cf_vars"]]
if (class(cf_vars) == "matrix"){
    if(dim(cf_vars)[1] == 1){
        cf_vars <- list(as.vector(cf_vars))
    } else {
        cf_vars <- t(cf_vars)
        cf_vars <- split(cf_vars, rep(1:ncol(cf_vars), each=nrow(cf_vars)))
        names(cf_vars) <- NULL
    }
}

# load and clean the data
df <- fread("FILEPATH")
df$age <- as.factor(df$age)
df$level_1 <- as.character(df$level_1)

ko <- fread("FILEPATH")

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

paste_model_lm <- function(x, y, i, usa_re){
    variables <- do.call("paste", c(as.list(x), sep=" + "))
    if (usa_re) {
        model <-  paste0("lmer(", y, " ~ ", variables, " + age + (1|level_4_nest)",
                         ", data=subset(df, ko[[", i, "]]), weights=weight)")
    } else {
        model <-  paste0("lmer(", y, " ~ ", variables, " + age + (1|level_1) + ",
                         "(1|level_2_nest) + (1|age_nest) + (1|level_3_nest)",
                         ", data=subset(df, ko[[", i, "]]), weights=weight)")
    }
    return(model)
}

extract_merMod <- function(merMod, df, usa_re){
    merMod_list <- list()
    merMod_list[["vcov"]] <- as.matrix(vcov(merMod))
    merMod_list[["fix_eff"]] <- data.frame(values=fixef(merMod))
    merMod_list[["ran_eff"]] <- update_ran_eff(ranef(merMod), df)
    if (usa_re) {
        row.names(merMod_list[["ran_eff"]][["level_4_nest"]]) <-
            paste0("_", row.names(merMod_list[["ran_eff"]][["level_4_nest"]]))
    } else {
        row.names(merMod_list[["ran_eff"]][["level_1"]]) <-
            paste0("_", row.names(merMod_list[["ran_eff"]][["level_1"]]))
    }
    return(merMod_list)
}

# wrapper in case errors come about because only one age group exists in a knockout
run_lmer_age_wrapper <- function(call){
    loginfo(call)
    out <- tryCatch({eval(parse(text=call))},
                    error=function(cond){eval(parse(text=gsub("\\+ age ", "", call)))})
    out
}

all_kos <- function(call, df, ko, usa_re){
    kos <- extract_merMod(run_lmer_age_wrapper(call), df, usa_re)
    kos
}

all_calls <- function(df, ko, calls, cores=4, usa_re){
    temp <- mclapply(calls, all_kos, df=df, ko=ko, usa_re=usa_re, mc.cores = cores)
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
                                  i = seq(1, ncol(ko), 3), usa_re=usa_re))
lm_cf_calls <- as.vector(sapply(cf_vars, paste_model_lm, y="lt_cf",
                                i = seq(1, ncol(ko), 3), usa_re=usa_re))

# We were seeing beta values dramatically in the wrong direction
#   when dropping the country nested random effects from the st-models.
#   Now, we're building the spacetime models identically to the linear
#   models, but dropping the country random effects before saving the Json.
drop_country_nest <- function(models){
    object <- models
    for(model in 1:length(object)){
        for(ko in 1:length(object[[model]])){
            object[[model]][[ko]]$ran_eff$level_3_nest <- NULL
        }
    }
    return(object)
}

loginfo("Running linear models")
lin_mods <- all_calls(df, ko, c(lm_rate_calls, lm_cf_calls), cores=cores_N, usa_re = usa_re)
loginfo("Dropping country nested random effects from linear models to use as spacetime")
space_mods <- drop_country_nest(lin_mods)

loginfo("Restructuring data and saving as JSON")
write_json(x=lin_mods, path=paste0(model_dir, "FILEPATH"), dataframe="columns", digits=14)
write_json(x=space_mods, path=paste0(model_dir, "FILEPATH"), dataframe="columns", digits=14)

loginfo("Finished!")
# shut down logging
logReset()
