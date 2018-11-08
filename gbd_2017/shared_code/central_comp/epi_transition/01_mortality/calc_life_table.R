################################################################################
## DESCRIPTION: Calculates full life table based on expected mx using shared functions from mortality team ##
## INPUTS: Expected mx ##
## OUTPUTS: Life tables for age groups 28, 2-20, 30-32, 235 ##
################################################################################

## DRIVE MACROS
rm(list = ls())
if (Sys.info()[1] == "Linux") {
    j <- "FILEPATH"
    h <- "FILEPATH"
    c <- "FILEPATH"
} else if (Sys.info()[1] == "Windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
    c <- "FILEPATH"
}

## LOAD DEPENDENCIES
source(paste0(c, "helpers/primer.R"))

## SCRIPT SPECIFIC FUNCTIONS
lifetable <- function(data, term_age = 110, preserve_u5 = 0, cap_qx = 0, force_cap = F, qx_diag_threshold = NULL) {
    if (Sys.info()[1] == "Windows") {
        root <- "FILEPATH"
    } else {
        root <- "FILEPATH"
    }

    ## checking if data table or data frame to return object of same format
    dframe <- F
    if (!is.data.table(data)) {
        dframe <- T
        data <- data.table(data)
    }

    ## make flexible with regard to age vs age_group_id, check to make sure all the correct age groups are present
    age_group_id <- F
    lt_ages <- data.table(get_age_map(type = "lifetable"))
    if ("age_group_id" %in% names(data)) {
        age_group_id <- T
        if ("age" %in% names(data)) stop("You have more than one age variable, delete one and try again")
        lt_ages <- lt_ages[, list(age_group_id, age_group_name_short)]
        if (!isTRUE(all.equal(unique(sort(data$age_group_id)), unique(lt_ages$age_group_id)))) stop("You don't have the correct age groups")
        data <- merge(lt_ages, data, by = c("age_group_id"), all.y = T)
        setnames(data, "age_group_name_short", "age")
    }

    ## make flexible with regard to sex and sex_id
    sex_id <- F
    if ("sex_id" %in% names(data)) {
        sex_id <- T
        if ("sex" %in% names(data)) stop("You have more than one sex identifier, delete one and try again")
        data[sex_id == 1, sex := "male"]
        data[sex_id == 2, sex := "female"]
        data[sex_id == 3, sex := "both"]
        data[, "sex_id" := NULL]
    }

    ## make flexible with regard to year and year_id
    year_id <- F
    if (!"year" %in% names(data)) {
        year_id <- T
        setnames(data, "year_id", "year")
    }

    ## order data
    data <- data[order(id, sex, year, age)]
    if ("qx" %in% names(data)) {
        data[, qx := as.numeric(qx)]
    }
    data[, mx := as.numeric(mx)]
    data[, ax := as.numeric(ax)]
    data[, age := as.numeric(age)]
    if (typeof(data$sex) != "character") data[, sex := as.character(sex)]

    ## can set up more assurances here (certain things uniquely identify, etc.)
    setkeyv(data, c("id", "sex", "year", "age"))
    num_duplicates <- length(duplicated(data)[duplicated(data) == T])
    if (num_duplicates > 0) stop(paste0("You have ", num_duplicates, " duplicates of your data over id_vars"))

    ## get length of intervals
    gen_age_length(data, terminal_age = term_age)
    setnames(data, "age_length", "n")

    ## qx
    if (preserve_u5 == 1) {
        data[age > 1, qx := (n * mx) / (1 + (n - ax) * mx)]
    }
    if (preserve_u5 == 0) {
        data[, qx := (n * mx) / (1 + (n - ax) * mx)]
    }

    ## setting qx to be 1 for the terminal age group
    data[age == max(age), qx := 1]

    if (cap_qx == 1) {
        if (!is.null(qx_diag_threshold)) {
            qx_over_threshold <- data[qx > qx_diag_threshold]
        }
        if (nrow(data[qx > 1.5]) > 0 & force_cap == F) {
            stop(paste0("Some values of qx are greater than 1.5"))
        } else {
            data[qx > 1, qx := 0.9999]
        }
    } else {
        if (nrow(data[qx > 1]) > 0) stop(paste0("Probabilities of death over 1, re-examine data, or use cap option. Max qx value is"), max(data$qx))
    }

    ## px
    data[, px := 1 - qx]

    ## lx
    data[, lx := 0]
    data[age == 0, lx := 100000]
    for (i in 1:length(unique(data$age))) {
        temp <- NULL
        temp <- data$lx * data$px
        temp <- c(0, temp[-length(temp)])
        data[, lx := 0]
        data[, lx := lx + temp]
        data[age == 0, lx := 100000]
    }

    ## dx
    setkey(data, id, sex, year, age)
    setnames(data, "n", "age_length")
    lx_to_dx(data, terminal_age = term_age)

    ## nLx
    gen_nLx(data, terminal_age = term_age)

    ## Tx
    gen_Tx(data, id_vars = c("id", "sex", "year", "age"))

    ## ex
    gen_ex(data)

    setnames(data, "age_length", "n")

    ## returning in same format
    if (sex_id == T) {
        data[sex == "male", sex_id := 1]
        data[sex == "female", sex_id := 2]
        data[sex == "both", sex_id := 3]
        data[, sex := NULL]
    }

    if (age_group_id == T) {
        data[, age := NULL]
    }

    if (year_id == T) {
        setnames(data, "year", "year_id")
    }

    if (dframe == T) {
        data <- as.data.frame(data)
    }

    if (!is.null(qx_diag_threshold)) {
        return(list(data, qx_over_threshold))
    } else {
        return(data)
    }
}


## PARSE ARGS
parser <- ArgumentParser()
parser$add_argument("--data_dir",
                    help = "Site where data will be stored",
                    default = "FILEPATH", type = "character"
)
parser$add_argument("--etmvid",
                    help = "Model version ID",
                    default = 12, type = "integer"
)
args <- parser$parse_args()
list2env(args, environment())
rm(args)


## LOAD RELEVANT DATA
mxdf <- et.getProduct(data_dir = data_dir, etmtid = 1, etmvids = etmvid, agids = c(28, 5:20, 30:32, 235), sids = 1:3, gbdids = 294, mean = T, process_dirs = "fits", scale = "normal")
agesdf <- get_age_map(type = "all") %>% as.data.table() %>% .[age_group_id %in% unique(mxdf$age_group_id)]
agesdf[, t := age_group_years_end - age_group_years_start]
agesdf[age_group_id == 235, t := 5]

## BODY
mxdf <- merge(mxdf, agesdf[, .(age_group_id, age = age_group_years_start, t)], by = "age_group_id")
mxdf[, qx := mx_to_qx(pred, t)][, ax := mx_qx_to_ax(pred, qx, t)]
mxdf <- mxdf[, .(age, sex = sex_id, year = sdi, mx = pred, ax, qx)]
mxdf[, id := seq(.N), by = .(age, sex, year)]

full_lt <- lifetable(mxdf, term_age = max(mxdf$age))

## FORMAT OUTPUTS TO MATCH PREVIOUS ORIGINAL GBD LT OUTPUTS
setnames(full_lt, c("sex", "year", "ex"), c("sex_id", "sdi", "pred"))
full_lt <- merge(full_lt, agesdf[, .(age = age_group_years_start, age_group_id)], by = "age")
full_lt <- full_lt[, .(sdi, age_group_id, sex_id = as.integer(sex_id), measure_id = 26, metric_id = 5, nLx, lx, ax, pred)][order(age_group_id, sex_id, sdi)]

## SAVE OUTPUTS
split_lts <- split(full_lt, by = c("age_group_id", "sex_id"))
lapply(split_lts, function(lt) saveRDS(lt, sprintf("%s/t1/v%d/life_table/MEAN_agid_%d_sid_%d_gbdid_294.RDs", data_dir, etmvid, unique(lt$age_group_id), unique(lt$sex_id))))

write.csv(full_lt, sprintf("%s/t1/v%d/summaries/summary_lt.csv", data_dir, etmvid), row.names = F)
