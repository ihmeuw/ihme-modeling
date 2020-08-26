# get the neccassary libraries
rm(list=ls())

library(lme4, lib.loc='FILEPATH')
library(data.table, lib.loc='FILEPATH')
library(parallel, lib.loc='FILEPATH')
library(gtools, lib.loc='FILEPATH')
library(car, lib.loc='FILEPATH')
# had to re-install the MySQL package on the new cluster, and it requires
# DBI > 0.4.0, so needed to reinstall that as well.
# FILEPATH is not writable, so put them into
# a different library directory. DBI must be loaded before RMySQL
library(DBI, lib.loc='FILEPATH')
library(RMySQL, lib.loc='FILEPATH')
library(jsonlite, lib.loc='FILEPATH')

options(lmerControl=list(check.nobs.vs.rankZ = "warning",
                         check.nobs.vs.nlev = "warning",
                         check.nobs.vs.nRE = "warning",
                         check.nlev.gtreq.5 = "warning",
                         check.nlev.gtr.1 = "warning"))

# get the model version id
args <- commandArgs(TRUE)
model_version_id <- args[1]
model_dir <- args[2]
setwd(model_dir)
DB <- args[3]
cores_N <- args[4]
outcome <- args[5]
password <- fromJSON("FILEPATH")[["password"]]
set.seed(model_version_id)
print(paste0("Running covariate selection for model version id: ",
             model_version_id))
date()

output_file <- file(paste0("FILEPATH", outcome, ".Rout"), open="wt")
sink(output_file)
sink(output_file, type="message")

max_model_calc <- 12000 # maximum number of calculations are cluster can handle
num_model_types <- 2 # number of model types spacetime and mixed

# load and clean the data
df <- fread ("FILEPATH", verbose=F)
df$age <- as.factor(df$age)
age_vars <- paste0("age", sort(unique(as.character(df$age))))
df$has_sub_nat <- as.numeric(df$location_id != df$country_id)
df$super_region <- as.character(df$super_region)

cv <- fread("FILEPATH")
cv$level <- cv$level - (min(cv$level) - 1)

test_model_convergence <- function(model){
    # hacky way to find model convergence because I cant find any documentation
    # on any sort of denotation that a model failed to converge
    status <- TRUE
    if(!is.null(model@optinfo$conv$lme4$messages)){
        status <- !any(grepl("failed to converge",
                             model@optinfo$conv$lme4$messages))
    }
    return(status)
}

check_for_nulls <- function(model_list) {
    # Check for null models and report
    nulls <- sum(unlist(lapply(model_list, is.null)))
    print("Found this many null models:")
    print(nulls)
    if (nulls != 0) quit(status=137)
}

pasteModel <- function(x, y){
    paste0("lmer(", y, " ~ ", x,
           " + age + (1|super_region) + ",
           "(1|region_nest) + (1|age_nest) + (1|country_nest)",
           ", data=df, weights=weight)")
}

createCalls <- function(new, oldModel=NULL, y){
    covs <- unlist(lapply(1:length(new), function(i)
        apply(combinations (n=length(new), r=i, v=new), MARGIN=1, function(x)
            do.call("paste", c(as.list(x), sep=" + ")))))
    if(is.null(oldModel)){
        return(pasteModel(covs, y))
    }
    else{
        n <- length(new)
        old <- do.call("paste", c(as.list(oldModel, covs), sep=" + "))
        pasteModel(paste(old, covs, sep=" + "), y)
    }
}

extract_mod <- function(model){
    model <- try(eval(parse(text=model)))
    model_list <- list()
    if (class(model)[[1]] != "lmerMod" | !test_model_convergence(model)){
        model_list[["Anova"]] <- NA
        model_list[["fix_eff"]] <- NA
        return (model_list)
    }
    model_list[["Anova"]] <- Anova(model)
    model_list[["fix_eff"]] <- fixef(model)[setdiff(names(fixef(model)[-1]), age_vars)]
    return (model_list)
}

name_to_pvalue <- function(vars, cv){
    sapply(vars, function(x) cv$p_value[cv$name==x])
}

validTest <- function(model_list, cv){
    if(length(model_list[["fix_eff"]])==0){
        return(FALSE)
    }
    if(is.na(model_list[["fix_eff"]])){
        return(FALSE)
    }
    v <- names(model_list[["fix_eff"]])
    expected <- sapply(v, function(x) cv$direction[cv$name == x])
    non0 <- expected != 0
    all(expected[non0] == (sign(model_list[["fix_eff"]])[non0])) &
        all(model_list[["Anova"]][["Pr(>Chisq)"]][0:(length(model_list[["Anova"]][["Pr(>Chisq)"]])-1)] <
                sapply(v, function(x) cv$p_value[cv$name==x]))
}

modelNames <- function(model_lists){
    lapply(model_lists, function(x) names(x[["fix_eff"]]))
}

needModel <- function(call, dv){
    any(sapply(dv, function(x) grepl(paste0(x, " "), call)))
}

getNeededCalls <- function(calls, dv){
    calls[sapply(calls, needModel, dv)]
}

complexOnly <- function(cSorted, cvNeeded, cv, base){
    vModels <- list(); needv <- cvNeeded; start <- 0; N <- length(cvNeeded)
    for(i in 1:(N-1)){
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

testModels <- function(model1=NULL, v, cv, y, cores){
    calls <- createCalls(v, model1, y)
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
    calls <- rev(createCalls(newv, model1, y)[-(1:length(newv))])
    complexOnly(calls, newv, cv, model1)
}

cvSelect <- function(df, cv, y, cores){
    vars <- lapply(1:max(cv$level), function(x) cv$name[cv$level==x])
    print("Testing level 1 covariates.")
    f <- testModels(NULL, vars[[1]], cv, y, cores)
    print("The models selected for level 1 covariates are:")
    print(f)
    print("Testing level 2 covariates.")
    if(2 %in% unique(cv$level)){
        lvl2 <- unlist(mclapply(f, function(x)
            testModels(x, vars[[2]], cv, y, 1), mc.cores=cores), recursive=F)
        print("The models selected for level 2 covariates are:")
        print(lvl2)
        f <- append(f, lvl2)
    }
    print("Testing level 3 covariates.")
    if(3 %in% unique(cv$level)){
        lvl3 <- unlist(mclapply(f, function(x)
            testModels(x, vars[[3]], cv, y, 1), mc.cores=cores), recursive=F)
        print("The models selected for level 3 covariates are:")
        print(lvl3)
        f <- append(f, lvl3)
    }

    f <- lapply(f, function(x) x[x!="1"])
    f <- f[sapply(f, function(x) length(x) != 0)]
    return(f)
}

vars <- cvSelect(df, cv, outcome, cores_N)
print("vars length:")
print(length(vars))

mci_to_cmvi <- function(model_covariate_id){
    call <- paste('SELECT covariate_model_version_id',
                  'FROM cod.model_covariate',
                  'WHERE model_covariate_id =',
                  as.character(model_covariate_id))
    con <- dbConnect(MySQL(), user='USERNAME', password=password, dbname="ADDRESS",
                     host=DB)
    res <- dbSendQuery(con, call)
    df <- fetch(res)
    dbDisconnect(dbListConnections(MySQL())[[1]])
    df[1,1]
}

banned_pairs <- function(model_version_id, priors){
    p <- as.data.frame(priors)
    call <- paste('SELECT banned_cov_pairs',
                  'FROM cod.model_version',
                  'WHERE model_version_id =',
                  as.character(model_version_id))
    con <- dbConnect(MySQL(), user='USERNAME', password=password, dbname="ADDRESS",
                     host=DB)
    res <- dbSendQuery(con, call)
    df <- fetch(res)
    dbDisconnect(dbListConnections(MySQL())[[1]])
    if (is.na(df[1,1])){
        return (list())
    }
    banned_models <- strsplit(strsplit(df[1,1], ";")[[1]], " ")
    lapply(banned_models, function(x) sapply(x, function(y)
        p[p$covariate_model_version_id  == mci_to_cmvi(y), "name"]))
}

holdout_number <- function(model_version_id){
    call <- paste('SELECT holdout_number',
                  'FROM cod.model_version',
                  'WHERE model_version_id = ', model_version_id)
    con <- dbConnect(MySQL(), user='USERNAME', password=password,
                     host=DB, dbname="ADDRESS")
    res <- dbSendQuery(con, call)
    df <- fetch(res)
    dbDisconnect(dbListConnections(MySQL())[[1]])
    df[1,1]
}

ban_list <- banned_pairs(model_version_id, cv)

for(l in ban_list){
    bad_vars <- c()
    for(j in 1:length(vars)){
        if(all(l %in% vars[j])){
            bad_vars <- c(bad_vars, j)
        }
    }
    vars <- vars[setdiff(1:length(vars), bad_vars)]
}

max_model_num <- max_model_calc / holdout_number(model_version_id) / num_model_types
max_num <- round(max_model_num * (length(vars) / length(vars)))

if(length(vars) == 0){
    vars <- list()
    write("", file=paste0("step_2/", "no_covariates_for_", outcome, ".txt"))
} else if (length(vars) > max_num){
    vars <- sample(vars, max_num)
    print("Too many models selected, dropping some")
}

all_models <- list(vars)
names(all_models) <- c(paste0(outcome, "_vars"))
write_json(x=all_models, path=paste0("step_2/cv_selected_", outcome, ".txt"))

print("Finished!!!")
date()
sink(type="message")
sink()
