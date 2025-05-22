rm(list = ls())

expected.rows <- 10334
min.deaths <- 100000
min.pct.sigtemps <- 0.05
min.pct.sigzones <- 0.50


library("tidyverse")
library("haven")
library("data.table")
library("feather")


source(paste0(FILEPATH, "get_cause_metadata.R"))

causeMeta <- get_cause_metadata(cause_set_id=4, gbd_round_id=6)[, c("cause_id", "cause_name", "acause", "sort_order", "level")]


zoneLimits <- round(fread(FILEPATH))
chn <- fread(FILEPATH)
full <- fread(FILEPATH)

counts <- rbind(select(chn, starts_with("n_")), select(full, starts_with("n_")), fill = T)
counts <- counts %>% replace(is.na(.), 0)

setDT(counts)

totals <- counts[, lapply(.SD, sum), .SDcols = names(counts)]
totals <- data.table(cause = names(totals), n = t(totals))
names(totals) <- c("cause", "n")

totals[, touse := 0]
totals[grepl("other", cause) == F & grepl("all_cause", cause)==F & grepl("gc", cause) == F & n >= min.deaths, touse := 1]
totals[, cause := gsub("^n_", "", cause)]

cause.list.raw <- totals[touse == 1, cause]
file.test <- sapply(cause.list.raw, function(cause) {file.exists(FILEPATH)})
cause.list <- names(file.test)[file.test == T]


sig.test <- function(cause, make.graph = T) {
  print(cause)

  file <- FILEPATH

  if (file.exists(file) == F) {
    return(data.table(cause = cause, flag = "missing file"))
  } else {
    # import draws
    tmp <- fread(file)

    if (nrow(tmp) < expected.rows) {
      return(data.table(cause = cause, flag = "missing rows"))

    } else {
      # rescale curves relative to minimum risk temperature
      tmp[, "mean_raw" := apply(.SD, 1, mean), .SDcols=paste0("draw_", 0:999)]
      tmp[, "lower_raw" := apply(.SD, 1, quantile, c(.025)), .SDcols=paste0("draw_", 0:999)]
      tmp[, "upper_raw" := apply(.SD, 1, quantile, c(.975)), .SDcols=paste0("draw_", 0:999)]
      tmp[, tmrel := sum(daily_temperature * (mean_raw == min(mean_raw))), by = "annual_temperature"]

      tmp[, paste0("draw_", 0:999) := lapply(.SD, function(x) {x - sum(x*(daily_temperature == tmrel))}), by = "annual_temperature", .SDcols = paste0("draw_", 0:999)]


      # find point estimate and bounds of 95% UI
      tmp[, "mean" := apply(.SD, 1, mean), .SDcols = paste0("draw_", 0:999)]
      tmp[, "lower" := apply(.SD, 1, quantile, c(.025)), .SDcols=paste0("draw_", 0:999)]
      tmp[, "upper" := apply(.SD, 1, quantile, c(.975)), .SDcols=paste0("draw_", 0:999)]
      tmp[, paste0("draw_", 0:999) := NULL]


      # find temperatures where RR is significantly different from null
      tmp[, n.temps := 1]
      tmp[, sig := upper*lower > 0]

      tmp[, counter := seq_len(.N)*sig, by=.(annual_temperature, rleid(sig))]
      tmp[, consec.sig := max(counter) / sum(n.temps), by = "annual_temperature"]


      if (make.graph == T) {
        tmp[, `:=` (ymin = min(c(lower, lower_raw))*sig, ymax = max(c(upper, upper_raw))*sig), by = "annual_temperature"]
        tmp[, meanTempLabel := factor(annual_temperature, levels = unique(annual_temperature), labels = paste0(unique(annual_temperature), "?C"))]


        tmp %>% filter(annual_temperature >= 6) %>%
          ggplot(aes(x = daily_temperature)) + geom_ribbon(aes(ymin = ymin, ymax = ymax), fill = "grey70", alpha = 0.4) +
          geom_line(aes(y = mean_raw), color = "grey70", linetype = "dashed") +
          geom_line(aes(y = lower_raw), color = "grey70", linetype = "dashed") +
          geom_line(aes(y = upper_raw), color = "grey70", linetype = "dashed") +
          geom_ribbon(aes(ymin = lower, ymax = upper), fill = "navy", alpha = 0.4) +
          geom_line(aes(y = mean), color = "navy") + geom_hline(yintercept = 0) +
          facet_wrap(~meanTempLabel, scales = "free") + theme_minimal() +
          xlab("Daily temperature (?C)") + ylab("Log relative risk") +
          ggtitle(causeMeta[acause == cause, cause_name])

        ggsave(paste0(FILEPATH, ".pdf"), width = 17, height = 11)
        ggsave(paste0(FILEPATH, ".png"), width = 11, height = 8.5)
      }

      # collapse by temp zone
      tmp <- tmp[, lapply(.SD, mean), by = "annual_temperature", .SDcols = c("sig", "consec.sig")]

      # clean up and return
      names(tmp) <- c("meanTemp", "pct.sig", "pct.consec.sig")
      tmp[, cause := cause]

      return(tmp)
    }
  }
}

test <- rbindlist(lapply(cause.list, sig.test), fill = T)

out <- test %>% group_by(cause, flag) %>%
        summarise(p05 = mean(pct.consec.sig >= 0.05),
                  p10 = mean(pct.consec.sig >= 0.10),
                  p20 = mean(pct.consec.sig >= 0.20))

out <- merge(out, totals, by = "cause", all.x = T)
out$touse <- NULL


write.csv(out, FILEPATH, row.names = F)