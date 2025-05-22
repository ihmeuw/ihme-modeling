# get the necessary libraries
rm(list=ls())

library(lme4)
library(data.table)
library(parallel)
library(gtools)
library(car)
library(DBI)
library(RMySQL)
library(jsonlite)
library(logging)
library(ini)

source("FILEPATH")

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
conn_def <- args[3]
cores_N <- args[4]
outcome <- args[5]
usa_re <- ifelse(as.numeric(args[6]) == 11, TRUE, FALSE)
include_cf <- as.numeric(as.logical(args[7]))
include_rates <- as.numeric(as.logical(args[8]))

set.seed(model_version_id)
setwd(model_dir)
max_model_calc <- 12000 # maximum number of calculations our cluster can handle
num_model_types <- 2 # number of model types spacetime and mixed
odbc <- read.ini("FILEPATH")[[conn_def]]

# set up logging
logReset()
basicConfig(level='DEBUG')
log_file <- paste0("logs/step_2_", outcome, "_covariate_selection.log")
suppressWarnings(file.remove(log_file))
addHandler(writeToFile, file=log_file)
loginfo(paste0("Running ", outcome, " covariate selection for model version ID ", model_version_id))

# load and clean the data
loginfo("Loading input data and priors")
df <- fread ("inputs/input_database_square.csv", verbose=F)
df$level_1 <- as.character(df$level_1)
age_spans <- get_age_spans()[age_group_id %in% df$age_group_id, ][order(age_group_years_start)]
df$age <- factor(df$age_group_id, levels = age_spans$age_group_id)
age_vars <- paste0("age", age_spans$age_group_id)

cv <- fread("inputs/priors.csv")
cv$level <- cv$level - (min(cv$level) - 1)

test_model_convergence <- function(model) {
    # parse model messages and warnings to determine convergence
    status <- TRUE
    if (class(model) == "try-error") {
        status <- FALSE
    } else if (any(grepl("fail(ed|ure) to converge", model@optinfo$conv$lme4$messages)) |
               any(grepl("fail(ed|ure) to converge", model@optinfo$warnings))) {
        status <- FALSE
    }
    return(status)
}

check_for_nulls <- function(model_list) {
    # Check for null models and report
    nulls <- sum(unlist(lapply(model_list, is.null)))
    loginfo(paste0("Found ", nulls, " null models"))
    if (nulls != 0) quit(status=137)
}

pasteModel <- function(x, y, usa_re) {
    # build formula string from outcome and covariate strings
    if (usa_re) {
        model <- paste0("lmer(", y, " ~ ", x, " + age + (1|level_4_nest)",
                        ", data=df, weights=weight)")
    } else {
        model <- paste0("lmer(", y, " ~ ", x, " + age + (1|level_1) + ",
                        "(1|level_2_nest) + (1|age_nest) + (1|level_3_nest)",
                        ", data=df, weights=weight)")
    }
    return(model)
}

createCalls <- function(new, oldModel=NULL, y, usa_re) {
    # create combinations of covariates and build formula strings for each
    covs <- unlist(lapply(1:length(new), function(i)
        apply(combinations (n=length(new), r=i, v=new), MARGIN=1, function(x)
            do.call("paste", c(as.list(x), sep=" + ")))))
    if(is.null(oldModel)) {
        return(pasteModel(covs, y, usa_re))
    }
    else{
        n <- length(new)
        old <- do.call("paste", c(as.list(oldModel, covs), sep=" + "))
        return(pasteModel(paste(old, covs, sep=" + "), y, usa_re))
    }
}

extract_mod <- function(model) {
    # build list of model attributes (anova and fixed_eff)
    model_list <- list()
    model_list[["model"]] <- model
    model <- try(eval(parse(text=model)))
    if (!test_model_convergence(model)) {
        model_list[["Anova"]] <- NA
        model_list[["fix_eff"]] <- NA
        return (model_list)
    }
    model_list[["Anova"]] <- Anova(model)
    model_list[["fix_eff"]] <- fixef(model)[setdiff(names(fixef(model)[-1]), age_vars)]
    return (model_list)
}

name_to_pvalue <- function(vars, cv) {
    # get p-value for given covariate_name_short
    return(sapply(vars, function(x) cv$p_value[cv$name==x]))
}

validTest <- function(model_list, cv) {
    # determine if test if valid, did model converge and p-value and coefficient
    # specifications hold true?
    if(length(model_list[["fix_eff"]])==0) {
        logdebug(paste0("Valid test: FALSE\n", model_list[["model"]], "\n- Model did not converge"))
        return(FALSE)
    } else if(any(is.na(model_list[["fix_eff"]]))) {
        logdebug(paste0("Valid test: FALSE\n", model_list[["model"]], "\n- Model did not converge"))
        return(FALSE)
    } else if(any(is.na(model_list[["Anova"]][["Pr(>Chisq)"]][0:(length(model_list[["Anova"]][["Pr(>Chisq)"]])-1)]))) {
        logdebug(paste0("Valid test: FALSE\n", model_list[["model"]], "\n- Model did not converge"))
        return(FALSE)
    }

    test_is_valid <- TRUE
    test_msg <- paste0("- Model converged with covariates: ", paste0(names(model_list[["fix_eff"]]), collapse = ", "))
    v <- names(model_list[["fix_eff"]])
    expected <- sapply(v, function(x) cv$direction[cv$name == x])
    non0 <- expected != 0
    test_msg <- c(test_msg,
                  paste0("- Covariate coefficient(s): ", paste0(model_list[["fix_eff"]][non0], collapse = ", "),
                         ". Expected direction(s): ", paste0(expected[non0], collapse = ", ")))
    if(!all(expected[non0] == (sign(model_list[["fix_eff"]])[non0]))) {
        test_is_valid <- FALSE
    }
    test_msg <- c(test_msg,
                  paste0("- P-Value(s): ", paste0(model_list[["Anova"]][["Pr(>Chisq)"]][0:(length(model_list[["Anova"]][["Pr(>Chisq)"]])-1)], collapse = ", "),
                         ". Expected less than: ", paste0(sapply(v, function(x) cv$p_value[cv$name==x]), collapse = ", ")))
    if (!all(model_list[["Anova"]][["Pr(>Chisq)"]][0:(length(model_list[["Anova"]][["Pr(>Chisq)"]])-1)] <
             sapply(v, function(x) cv$p_value[cv$name==x]))) {
        test_is_valid <- FALSE
    }
    test_msg <- paste0(c(paste0("Valid test: ", test_is_valid, "\n", model_list[["model"]]), test_msg), collapse = "\n")
    logdebug(test_msg)
    return(test_is_valid)
}

modelNames <- function(model_lists) {
    # get name of fixed_effect for a given list of models
    return(lapply(model_lists, function(x) names(x[["fix_eff"]])))
}

needModel <- function(call, dv) {
    return(any(sapply(dv, function(x) grepl(paste0(x, " "), call))))
}

getNeededCalls <- function(calls, dv) {
    return(calls[sapply(calls, needModel, dv)])
}

complexOnly <- function(cSorted, cvNeeded, cv, base) {
    vModels <- list(); needv <- cvNeeded; start <- 0; N <- length(cvNeeded)
    for(i in 1:(N-1)) {
        sCalls <- cSorted[(start+1):(start+choose(N,(N - (i-1))))]
        start <- start + choose(N,(N - (i-1)))
        sCalls <- getNeededCalls(sCalls, needv)
        sModels <- lapply(sCalls, function(x) extract_mod(x))
        check_for_nulls(sModels)
        keep <- sapply(sModels, validTest, cv); keep[is.na(keep)] <- FALSE
        vSubModels <- modelNames(sModels[keep])
        vModels <- append(vModels, vSubModels)
        for(m in vModels) needv <- setdiff(needv, m)
        if(length(needv) == 0) break
    }
    needv <- lapply(needv, function(x) c(base, x))
    return(append(vModels, needv))
}

testModels <- function(model1=NULL, v, cv, y, cores, usa_re) {
    calls <- createCalls(v, model1, y, usa_re)
    if(!is.null(model1)) calls <- calls[1:length(v)]
    models <- mclapply(calls, function(x) extract_mod(x),
                       mc.cores=cores)
    check_for_nulls(models)
    keep <- sapply(models, validTest, cv); keep [is.na(keep)] <- FALSE
    vModels <- modelNames(models[keep])
    if(length(vModels)==0 & is.null(model1)) return(list("1"))
    if(length(vModels)==0) return(list())
    if(length(vModels)==1 | is.null(model1)) return(vModels)
    newv <- sapply(vModels,function(x) intersect(x,v))
    vModels <- list(); needv <- newv; start <- 1
    calls <- rev(createCalls(newv, model1, y, usa_re)[-(1:length(newv))])
    return(complexOnly(calls, newv, cv, model1))
}

cvSelect <- function(df, cv, y, cores, usa_re) {
    vars <- lapply(1:max(cv$level), function(x) cv$name[cv$level==x])
    loginfo("Testing level 1 covariates")
    f <- testModels(NULL, vars[[1]], cv, y, cores, usa_re)
    loginfo(paste0("Models selected for level 1 covariates:\n- ",
                   paste0(f, collapse = "\n- ")))
    if(2 %in% unique(cv$level)) {
        loginfo("Testing level 2 covariates")
        lvl2 <- unlist(mclapply(f, function(x)
            testModels(x, vars[[2]], cv, y, 1, usa_re), mc.cores=cores), recursive=F)
        loginfo(paste0("Models selected for level 2 covariates:\n- ",
                       paste0(lvl2, collapse = "\n- ")))
        f <- append(f, lvl2)
    }
    if(3 %in% unique(cv$level)) {
        loginfo("Testing level 3 covariates")
        lvl3 <- unlist(mclapply(f, function(x)
            testModels(x, vars[[3]], cv, y, 1, usa_re), mc.cores=cores), recursive=F)

        loginfo(paste0("Models selected for level 3 covariates:\n- ",
                       paste0(lvl3, collapse = "\n- ")))
        f <- append(f, lvl3)
    }

    f <- lapply(f, function(x) x[x!="1"])
    f <- f[sapply(f, function(x) length(x) != 0)]
    return(f)
}

if ((outcome == "ln_rate" & include_rates == 1) |
    (outcome == "lt_cf" & include_cf == 1)) {
    vars <- cvSelect(df, cv, outcome, cores_N, usa_re)
    loginfo(paste0(length(vars), " models selected"))
} else {
    vars <- c()
    loginfo(paste0("Skipping covariate selection for ", outcome))
}

mci_to_cmvi <- function(model_covariate_id) {
    # map model_covariate_id (a CoD term and entity in cod.model_covariate) to
    # covariate_model_version_id (a CoD term in in cod.model_covariate for
    # covariate.model_version_id).
    call <- paste('SELECT covariate_model_version_id',
                  'FROM cod.model_covariate',
                  'WHERE model_covariate_id =',
                  as.character(model_covariate_id))
    con <- dbConnect(RMySQL::MySQL(), user=odbc$user, password=odbc$password,
                     host=odbc$server)
    covariate_model_version_id <- dbGetQuery(con, call)$covariate_model_version_id
    dbDisconnect(con)
    return(covariate_model_version_id)
}

banned_pairs <- function(model_version_id, priors) {
    # build list of banned covariate pairs model_covariate_id:covariate_name_short
    # for a given model_version_id
    call <- paste('SELECT banned_cov_pairs',
                  'FROM cod.model_version',
                  'WHERE model_version_id =',
                  as.character(model_version_id))
    con <- dbConnect(RMySQL::MySQL(), user=odbc$user, password=odbc$password,
                     host=odbc$server)
    banned_cov_pairs <- dbGetQuery(con, call)$banned_cov_pairs
    dbDisconnect(con)
    if (is.na(banned_cov_pairs)) return(list())
    banned_models <- strsplit(strsplit(banned_cov_pairs, ";")[[1]], " ")
    return(lapply(banned_models, function(x) sapply(x, function(y)
        priors[covariate_model_id  == mci_to_cmvi(y), ]$name)))
}

holdout_number <- function(model_version_id) {
    # get holdout_number for a given model_version_id
    call <- paste('SELECT holdout_number',
                  'FROM cod.model_version',
                  'WHERE model_version_id = ', model_version_id)
    con <- dbConnect(RMySQL::MySQL(), user=odbc$user, password=odbc$password,
                     host=odbc$server)
    holdout_number <- dbGetQuery(con, call)$holdout_number
    dbDisconnect(con)
    return(holdout_number)
}

loginfo("Dropping any banned pairs")
ban_list <- banned_pairs(model_version_id, cv)
for(banned in ban_list) {
    drop_vars <- c()
    for (i in 1:length(vars)) {
        if (all(banned %in% vars[[i]])) {
            drop_vars <- c(drop_vars, i)
        }
    }
    if (!is.null(drop_vars)) vars <- vars[-drop_vars]
}

max_model_num <- max_model_calc / holdout_number(model_version_id) / num_model_types
max_num <- round(max_model_num * (length(vars) / length(vars)))

if(length(vars) == 0) {
    vars <- list()
    write("", file=paste0("step_2/", "no_covariates_for_", outcome, ".txt"))
} else if (length(vars) > max_num) {
    vars <- sample(vars, max_num)
    loginfo(paste0("Too many models selected, max is ", max_num, ", dropping some"))
}

loginfo("Saving JSON of selected models")
all_models <- list(vars)
names(all_models) <- c(paste0(outcome, "_vars"))
write_json(x=all_models, path=paste0("step_2/cv_selected_", outcome, ".txt"))

loginfo("Finished!")
# shut down logging
logReset()
