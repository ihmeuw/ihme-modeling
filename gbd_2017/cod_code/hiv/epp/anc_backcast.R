
### Setup
rm(list=ls())
windows <- Sys.info()[1][["sysname"]]=="Windows"
root <- ifelse(windows,"","")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0(ifelse(windows, "", paste0("", user)), "")


## Packages
library(data.table); library(ggplot2);library(lme4); library(arm); library(mvtnorm); library(jsonlite)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {
	loc <- args[1]
} else {
	loc <- "TZA"
}

### Paths
anc.path <- paste0(root, "")
anc.path <- paste0("")
all.anc.path <- paste0(root, "")
dt.dir <- paste0("")
plot.dir <- paste0("")
dir.create(dt.dir, recursive = T, showWarnings = F)
dir.create(plot.dir, recursive = T, showWarnings = F)

dt.path <- paste0(dt.dir, loc, "")
plot.path <- paste0(plot.dir, loc, "")
n.plot.path <- paste0(plot.dir, loc, "")


### Functions
source(paste0(root, ""))
logit <- function(x) log(x/(1-x))
expit <- function(x) exp(x)/(1+exp(x))

### Tables
loc.table <- data.table(get_locations(hiv_metadata = T))
high.risk.list <- loc.table[epp == 1 & collapse_subpop == 0 & !grepl("ZAF", ihme_loc_id) & !grepl("KEN", ihme_loc_id), ihme_loc_id]

### Code
all.anc.dt <- fread(all.anc.path)
# Load adjacency data
adj.list <- fromJSON(paste0(root, ""))
loc.name <- loc.table[ihme_loc_id == loc, location_name]
cnat <- strsplit(loc, "_")[[1]][1]
nat.name <- loc.table[ihme_loc_id == nat, location_name]
if(loc %in% high.risk.list) {
	mean.dt <- all.anc.dt[ihme_loc_id == loc, .(mean = weighted.mean(mean, n)), by = c("subpop", "year_id")]
	subpop.max <- rbindlist(lapply(unique(mean.dt$subpop), function(spop) {
		subp.dt <- mean.dt[subpop == spop]
		out.dt <- subp.dt[mean == max(mean), .(subpop, year_id)]
		setnames(out.dt, "subpop", "subp")
	}))
} else {
	if(loc.name %in% names(adj.list)) {
		neigh <- loc.table[location_name %in% adj.list[[loc.name]], ihme_loc_id]
	} else if(nat.name %in% names(adj.list)) {
		neigh <- loc.table[location_name %in% adj.list[[nat.name]], ihme_loc_id]
	} else {
		neigh <- c()
	}
	all.anc.dt[, nat := tstrsplit(ihme_loc_id, "_")[[1]]]
	subset.anc.dt <- all.anc.dt[nat %in% c(neigh) | grepl(cnat, ihme_loc_id)]
	fit1 <- lmer(mean ~ (1|clinic) + (1|ihme_loc_id) + (1|year_id), data = subset.anc.dt)
	max.year <- as.integer(rownames(ranef(fit1)$year_id)[which(ranef(fit1)$year_id == max(ranef(fit1)$year_id))])
}

# Prep data
load(anc.path)
out.dt <- data.table()
for(subpop in names(result)) {
	anc.prev <- as.data.table(result[[subpop]]$likdat$anclik.dat$anc.prev, keep.rownames = T)
	anc.prev[, n_site := rowid(rn)]
	if(max(anc.prev$n_site) > 1) {
		sites <- unique(anc.prev[n_site > 1, rn])
		anc.prev[rn %in% sites, rn := paste0(rn, "_", n_site)]
	}
	anc.prev[, n_site := NULL]
	prev.melt <- melt(anc.prev, id.vars = "rn", variable.name = "year_id")
	setnames(prev.melt, "value", "mean")
	anc.n <- as.data.table(result[[subpop]]$likdat$anclik.dat$anc.n, keep.rownames = T)
	anc.n[, n_site := rowid(rn)]
	if(max(anc.n$n_site) > 1) {
		sites <- unique(anc.n[n_site > 1, rn])
		anc.n[rn %in% sites, rn := paste0(rn, "_", n_site)]
	}
	anc.n[, n_site := NULL]
	n.melt <- melt(anc.n, id.vars = "rn", variable.name = "year_id")
	setnames(n.melt, "value", "n")
	anc.dt <- merge(prev.melt, n.melt)
	# any(is.na(merge.dt$mean) != is.na(merge.dt$n))
	anc.dt <- anc.dt[!is.na(mean)]
	anc.dt[, year_id := as.integer(as.character(year_id))]
	setnames(anc.dt, "rn", "clinic")

	table(anc.dt[, (year_id)])
	anc.dt[mean == 0, mean := (mean * n + 0.5) / (n + 1)]
	anc.dt[, t := year_id - min(year_id)]
	mean.dt <- anc.dt[, .(mean = weighted.mean(mean, n)), by = "t"]
	if(loc %in% high.risk.list) {
		max.year <- subpop.max[subp == subpop, year_id]
	}
	max.t <- max.year - min(anc.dt$year_id)
	if(grepl("ZAF", loc)) {
		max.t <- 2001 - min(anc.dt$year_id)
	}
	anc.dt[, b1 := ifelse(t < max.t, max.t - t, 0)]
	anc.dt[, b2 := ifelse(t < max.t, 0, t - max.t)]
	anc.dt <- anc.dt[order(n)]
	anc.dt[, sd := sqrt(mean * (1 - mean) / n)]
	if(nrow(anc.prev) == 1) {	
		anc.dt[, pred := "Data"]
		bound.dt <- anc.dt
		bound.dt <- bound.dt[order(clinic, year_id)]
		bound.dt[, subpopulation := subpop]
		out.dt <- rbind(out.dt, bound.dt)		
	} else {
		## Fit model
		# Clinic fixed intercepts with a linear spline on time with a single knot set at the peak 
		# weighted average of observered prevalence in the data. Logit link function (binomial) with 
		# weights set using the inverse variance
		fit <- glm(mean ~ clinic + b1 + b2, data = anc.dt, family = quasibinomial(link = logit), weights = n)
		# fit <- glm(mean ~ clinic + b1 + b2 + b1*clinic + b2*clinic, data = anc.dt, family = quasibinomial(link = logit), weights = n)


		# Prep prediction data
		years <- min(anc.dt$t):max(anc.dt$t)
		potential.site.years <- data.table(expand.grid(years, unique(anc.dt$clinic)))
		colnames(potential.site.years) <- c("t", "clinic")
		observed.site.years <- anc.dt[, .(t, clinic)]
		observed.site.years[, obs := 1]
		merge.dt <- merge(potential.site.years, observed.site.years, by = c("t", "clinic"), all.x = T)
		pred.dt <- merge.dt[is.na(obs)]
		pred.dt[, obs := NULL]
		pred.dt[, b1 := ifelse(t < max.t, max.t - t, 0)]
		pred.dt[, b2 := ifelse(t < max.t, 0, t - max.t)]

		# Predict
		pred.dt[, year_id := t + min(anc.dt$year_id)]

		## Simulate to get effective sample size
		samp = 1000

		mv.samp <- rmvnorm(samp, summary(fit)$coef[, 1], vcov(fit))

		# Intercept
		int.samp <- mv.samp[,"(Intercept)"]
		# b1
		if("b1" %in% rownames(summary(fit)$coef)) {
			b1.samp <- mv.samp[, "b1"]
		} else {
			b1.samp <- rep(0, samp)
		}
		# b2 
		if("b2" %in% rownames(summary(fit)$coef)) {
			b2.samp <- mv.samp[, "b2"]
		} else {
			b2.samp <- rep(0, samp)
		}

		# summary.dt <- anc.dt[,.(n = .N, mean = mean(n)), by = clinic]
		# merge.dt <- merge(summary.dt, as.data.table(se.ranef(fit)$clinic, keep.rownames = T)[, clinic := rn], by = "clinic")
		# merge.dt[, rn := NULL]

		for(c.clinic in unique(pred.dt$clinic)) {
			# Clinic random intercepts
			# clinic.samp <- rnorm(samp, ranef(fit, condVar = T)$clinic[c.clinic,], 
			# 					 se.ranef(fit)$clinic[c.clinic,])
			clinic.name <- paste0("clinic", c.clinic)
			if(clinic.name %in% rownames(summary(fit)$coef)) {
				clinic.idx <- which(clinic.name ==rownames(summary(fit)$coef))
				clinic.samp <- mv.samp[, clinic.idx]
			} else {
				clinic.samp <- rep(0, samp)
			}
			
			# Prediction
			clinic.dt <- pred.dt[clinic == c.clinic]
			
			beta <- cbind(int.samp, b1.samp, b2.samp, clinic.samp)
			X <- cbind(rep(1, nrow(clinic.dt)), clinic.dt$b1, clinic.dt$b2, rep(1, nrow(clinic.dt)))
			preds <- beta %*% t(X)
			probs <- exp(preds) / (1 + exp(preds))
			mean.probs <- colMeans(probs)
			# var.probs <- apply(probs, 2, var)
			# bin.var <- mean.probs * (1 - mean.probs)
			# pred.n <- (mean.probs * (1 - mean.probs)) / var.probs
			# pred.dt[clinic == c.clinic, n := pred.n]
			pred.dt[clinic == c.clinic, mean := mean.probs]
			for(i in 1:samp) {
				pred.dt[clinic == c.clinic, paste0("draw_", i) := t(probs)[, i]]
			}
		}

		## Sample size model
		n.fit <- glm((n / (max(n) + 1)) ~ 0 + clinic + t, data = anc.dt, family = binomial(link = logit))
		n.pred <- expit(predict(n.fit, pred.dt)) * (max(anc.dt$n) + 1)
		pred.dt[, n := n.pred]

		# Combine
		anc.dt[, pred := "Data"]
		pred.dt[, pred := "Predicted"]
		bound.dt <- rbind(anc.dt, pred.dt, fill = T)
		bound.dt <- bound.dt[order(clinic, year_id)]
		bound.dt[, subpopulation := subpop]

		out.dt <- rbind(out.dt, bound.dt)
	}

}

# Write
write.csv(out.dt, dt.path, row.names = F)
### End