##########################################################
## MAIN FIT SCRIPT
##########################################################

rm(list = ls())
if (Sys.info()[1] == "Linux") {
    j <- "FILEPATH"
    h <- "FILEPATH"
    c <- "FILEPATH"
} else if (Sys.info()[1] == "Darwin") {
    j <- "FILEPATH"
    h <- "FILEPATH"
}

## LOAD DEPENDENCIES
source(paste0(c, "/helpers/primer.R"))

## ############################################
## Function for GAM FIT  ######################
## ############################################
fitPredMod <- function(datadf, model_type, model_family, smoothing_param, use_weights) {
    message(paste0("Fitting ", paste(model_type, model_family, smoothing_param, use_weights, sep = ";")))

    datadf <- copy(datadf)
    print(datadf)

    if (!use_weights) datadf[, w := 1]

    if (model_type == "pspline") {
        library(scam, lib.loc = "FILEPATH")
        mod <- bam(formula = val ~ s(sdi, bs = model_shape) + offset(log(pop)), data = datadf, family = model_family)
        summary(mod)

    } else if (as.character(model_type) == "loess") {
        library(gam)
        form <- "val ~ lo(sdi, span = smoothing_param)"
        if (model_family == "poisson") form <- paste0(form, " + offset(log(pop))")
        mod <- gam(formula = as.formula(form), data = datadf, family = as.character(model_family))

    } else if (as.character(model_type) == "glm") {
        print(datadf)
        form <- "val ~ sdi"
        if (model_family == "poisson") form <- paste0(form, " + offset(log(pop))")
        mod <- glm(formula = as.formula(form), data = datadf, weights = w, family = as.character(model_family))
    }

    preddf <- data.table(expand.grid(
        etmtid = etmtid,
        etmvid = etmvid,
        age_group_id = agid,
        sex_id = sid,
        measure_id = unique(datadf$measure_id),
        metric_id = unique(datadf$metric_id),
        gbd_id = gbdid,
        sdi = seq(0, 1, .005),
        pop = 1e4
    ))

    preddf[, pred := predict(mod, newdata = preddf, type = "response")]
    if (model_family == "poisson") preddf[, pred := pred / pop]

    default <- paste(parammap$model_type, parammap$model_family, parammap$smoothing_param, parammap$use_weights, sep = ";")
    preddf[, model := paste(model_type, model_family, smoothing_param, use_weights, sep = ";")]
    preddf[model == default, model := paste0(model, "- BEST")]

    return(preddf)
}

## ############################################
## PARSE ARGUMENTS ############################
## ############################################

parser <- ArgumentParser()
parser$add_argument("--data_dir",
                    help = "Site where data will be stored",
                    default = "FILEPATH", type = "character"
)
parser$add_argument("--etmtid",
                    help = "Model type ID",
                    default = 7, type = "integer"
)
parser$add_argument("--etmvid",
                    help = "Model version ID",
                    default = 9, type = "integer"
)
parser$add_argument("--agid",
                    help = "Age group ID",
                    default = 14, type = "integer"
)
parser$add_argument("--sid",
                    help = "Sex ID",
                    default = 1, type = "integer"
)
parser$add_argument("--resub",
                    help = "Whether job is being resubmitted",
                    default = 0, type = "integer"
)
args <- parser$parse_args()

print(args)
list2env(args, environment())
rm(args)

taskid <- ifelse(interactive(), loadTask(data_dir, etmtid, etmvid), as.numeric(Sys.getenv("SGE_TASK_ID")))
print(taskid)
print(resub)

whichtask <- ifelse(as.logical(resub), "resub_task_id", "task_id")
parammap <- readRDS(sprintf("%s/t%d/v%d/submitmap.RDS", data_dir, etmtid, etmvid))[age_group_id == agid & sex_id == sid & get(whichtask) == taskid]
setnames(parammap, c("age_group_id", "sex_id", "gbd_id"), c("agid", "sid", "gbdid"))
for (col in names(parammap)) {
    if (!is.character(parammap[[col]])) assign(col, eval(parse(text = as.character(parammap[[col]]))), environment()) else assign(col, as.character(parse(text = as.character(parammap[[col]]))), environment())
}

### Load outputs
mvid <- readRDS(sprintf("%s/t%d/v%d/dem_inputs/versiondf.RDS", data_dir, etmtid, etmvid)) %>% .$input_version_id
if (etmtid %in% c()) trans <- "none"
if (etmtid %in% c(2, 3, 7)) trans <- "logit"
if (etmtid %in% c(1, 4, 5, 101)) trans <- "log"
if (model_family != "gaussian") trans <- "none"
message(sprintf("agid: %d sid: %d", agid, sid))

df <- et.getProduct(data_dir = data_dir, etmtid = etmtid, etmvids = etmvid, agids = agid, sids = sid, gbdids = gbdid, obs = T, scale = model_transform, fl = model_floor, process_dirs = "gbd_inputs")
df <- df[model_use == T]

popsdf <- readRDS(sprintf("%s/t%d/v%d/dem_inputs/popsdf.RDS", data_dir, etmtid, etmvid))
df <- merge(df, popsdf, by = c("location_id", "year_id", "age_group_id", "sex_id"))
if (model_family == "poisson") df[, val := val * pop]

specdf <- rbind(data.table(model_type = "glm", model_family = model_family, smoothing_param = NA_real_, use_weights = use_weights),
                data.table(expand.grid(model_type = model_type, model_family = model_family, smoothing_param = c(smoothing_param), use_weights = use_weights)),
                use.names = T, fill = T
)

preddf <- mapply(
    FUN = fitPredMod, MoreArgs = list(datadf = df), model_type = specdf$model_type, model_family = specdf$model_family, smoothing_param = specdf$smoothing_param, use_weights = specdf$use_weights,
    SIMPLIFY = F
) %>% rbindlist(., use.names = T, fill = T)

if (model_family == "gaussian" & etmtid %in% c(4:5, 101)) preddf[, pred := exp(pred)]
if (etmtid %in% c(2, 3, 7)) preddf[, pred := inv.logit(pred)]

assert_values(preddf[model %like% "BEST"], colnames = "pred", test = "not_na")
assert_values(preddf[model %like% "BEST"], colnames = "pred", test = "gte", test_val = 0)

saveRDS(preddf[model %like% "BEST", !c("model", "pop")], sprintf("%s/t%d/v%d/fits/MEAN_agid_%d_sid_%d_gbdid_%d.RDs", data_dir, etmtid, etmvid, agid, sid, gbdid))
