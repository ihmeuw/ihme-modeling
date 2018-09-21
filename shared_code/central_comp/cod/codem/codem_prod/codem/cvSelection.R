# get the neccassary libraries
rm(list=ls())
.libPaths("LIBRARY_PATH")
library (lme4); library(data.table); library(parallel); library(gtools)
library(car); library(jsonlite); library(RMySQL)

options(lmerControl=list(check.nobs.vs.rankZ = "warning",
                         check.nobs.vs.nlev = "warning",
                         check.nobs.vs.nRE = "warning",
                         check.nlev.gtreq.5 = "warning",
                         check.nlev.gtr.1 = "warning"))

print(paste0("Running covariate selection for model version id: ",
             rev(strsplit(getwd(), "/")[[1]])[1]))
date()

# get the model version id
args <- commandArgs(TRUE)
model_version_id <- args[1]
DB <- args[2]
model_dir <- args[3]
password <- fromJSON("FILEPATH")[["password"]]
set.seed(model_version_id)

output_file <- file("FILEPATH", open="wt")
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

#detect other cores
cores_N <- min(detectCores(), 30)
print(cores_N)

test_model_convergence <- function(model){
    status <- TRUE
    if(!is.null(model@optinfo$conv$lme4$messages)){
        status <- !any(grepl("failed to converge",
                             model@optinfo$conv$lme4$messages))
    }
    return(status)
}

pasteModel <- function(x, y){
    paste0("lmer(", y, " ~ ", x,
           " + age + (1|super_region) + ",
           "(1|region_nest) + (1|age_nest) + (1|country_nest)",
           ", data=df)")
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
    f <- testModels(NULL, vars[[1]], cv, y, cores)
    if(2 %in% unique(cv$level)){
        lvl2 <- unlist(mclapply(f, function(x)
            testModels(x, vars[[2]], cv, y, 1), mc.cores=cores), recursive=F)
        f <- append(f, lvl2)
    }
    if(3 %in% unique(cv$level)){
        lvl3 <- unlist(mclapply(f, function(x)
            testModels(x, vars[[3]], cv, y, 1), mc.cores=cores), recursive=F)
        f <- append(f, lvl3)
    }

    f <- lapply(f, function(x) x[x!="1"])
    f <- f[sapply(f, function(x) length(x) != 0)]
    return(f)
}

cf_vars <- cvSelect(df, cv, "lt_cf", cores_N)
rate_vars <- cvSelect(df, cv, "ln_rate", cores_N)
print("cf vars length:")
print(length(cf_vars))
print("rate vars length:")
print(length(rate_vars))

mci_to_cmvi <- function(model_covariate_id){
    call <- paste('SELECT covariate_model_version_id',
                  'FROM cod.model_covariate',
                  'WHERE model_covariate_id =',
                  as.character(model_covariate_id))
    con <- dbConnect(MySQL(), user='USERNAME', password=password, dbname="DB_NAME",
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
    con <- dbConnect(MySQL(), user='USERNAME', password=password, dbname="DB_NAME",
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
    con <- dbConnect(MySQL(), user="USERNAME", password=password,
                     host=DB, dbname="DB_NAME")
    res <- dbSendQuery(con, call)
    df <- fetch(res)
    dbDisconnect(dbListConnections(MySQL())[[1]])
    df[1,1]
}

ban_list <- banned_pairs(model_version_id, cv)

for(l in ban_list){
    bad_cf <- c()
    for(j in 1:length(cf_vars)){
        if(all(l %in% cf_vars[j])){
            bad_cf <- c(bad_cf, j)
        }
    }
    bad_rate <- c()
    for(j in 1:length(rate_vars)){
        if(all(l %in% rate_vars[j])){
            bad_rate <- c(bad_rate, j)
        }
    }
    cf_vars <- cf_vars[setdiff(1:length(cf_vars), bad_cf)]
    rate_vars <- rate_vars[setdiff(1:length(rate_vars), bad_rate)]
}

max_model_num <- max_model_calc / holdout_number(model_version_id) / num_model_types
max_rate_num <- round(max_model_num * (length(rate_vars) / length(c(rate_vars, cf_vars))))
max_cf_num <- round(max_model_num * (length(cf_vars) / length(c(rate_vars, cf_vars))))

if (length(cf_vars) > max_cf_num){
    cf_vars <- sample(cf_vars, max_cf_num)
}

if (length(rate_vars) > max_rate_num){
    rate_vars <- sample(rate_vars, max_rate_num)
}

all_models <- list(rate_vars, cf_vars)
names(all_models) <- c("rate_vars", "cf_vars")
am_JSON <- toJSON(all_models)
sink("FILEPATH"); cat(am_JSON); sink()

print("Finished!!!")
date()
sink(type="message")
sink()
