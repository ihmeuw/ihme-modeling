require(ggplot2)
require(plyr)
require(data.table)
require(reshape2)

plot_effects <- function(mvid, location_id, year, sex, indir, outdir=".") {
    prior <- fread(sprintf(
        "%s/%s/full/locations/%s/inputs/%s/%s/effect.csv",
        indir, mvid, location_id, sex, year))
    rate_prior <- fread(sprintf(
        "%s/%s/full/locations/%s/inputs/%s/%s/rate.csv",
        indir, mvid, location_id, sex, year))
    post <- fread(sprintf(
        "%s/%s/full/locations/%s/outputs/%s/%s/post_ode_summary.csv",
        indir, mvid, location_id, sex, year))

    prior[, effect:=paste(effect, integrand, name, sep="_")]
    rate_prior[, effect:=paste(type, age, sep="_")]
    rate_prior[grepl("iota", effect), integrand:="incidence"]
    rate_prior[grepl("rho", effect), integrand:="remission"]
    rate_prior[grepl("chi", effect), integrand:="mtexcess"]
    rate_prior[grepl("omega", effect), integrand:="mtother"]
    rate_prior$age <- NULL
    prior <- rbind(prior, rate_prior, fill=T)

    plotdf <- merge(prior, post, by="effect", suffixes=c(".prior",".post"))
    plotdf <- rename(plotdf, c("lower"="lower.prior", "upper"="upper.prior", "post_lower"="lower.post", "post_upper"="upper.post"))
    plotdf <- plotdf[!grepl("^gamma_", effect)]
    if(location_id==1){
        write.csv(plotdf, sprintf("%s/global_posterior_effects.csv", outdir), row.names=F)
    }
    plotdf <- plotdf[, c("effect", "mean.prior", "lower.prior", "upper.prior", "mean.post", "lower.post", "upper.post"), with=F]
    plotdf <- plotdf[!grepl("iota|rho|chi|omega", effect)]

    plotdf <- melt(plotdf, id.vars="effect")
    plotdf[, metric:=unlist(lapply(strsplit(as.character(plotdf$variable), ".", fixed=T), function(x) x[1]))]
    plotdf[, stage:=unlist(lapply(strsplit(as.character(plotdf$variable), ".", fixed=T), function(x) x[2]))]

    plotdf <- dcast(plotdf, effect+stage ~ metric, value.var="value")
    plotdf$stage <- factor(plotdf$stage, c("prior","post"))
    plotdf$effect <- as.character(plotdf$effect)
    plotdf$effect <- factor(plotdf$effect, sort(plotdf$effect, decreasing=T))
    plotdf <- plotdf[order(plotdf$effect, plotdf$stage), ]
    plotdf <- data.table(plotdf)

    plotdf[, c("mean", "lower", "upper"):=list(as.numeric(mean), as.numeric(lower), as.numeric(upper))]
    plot_title <- sprintf("mvid: %s, locid: %s, year: %s, sex: %s", mvid, location_id, year, sex)
    p <- ggplot(data=plotdf, aes(x=factor(effect), y=mean, ymin=lower, ymax=upper, color=stage))
    p <- p + xlab("Effects") + ylab("Range") + ggtitle(plot_title)
    p <- p + geom_point(size=3) + geom_errorbar(size=1) + scale_y_continuous(limits=c(-2, 2)) + coord_flip() + theme_bw()

    plotfile <- sprintf("%s/effect_plot_%s_%s_%s_%s.pdf", outdir, mvid, location_id, year, sex)
    ggsave(p, file=plotfile, height=12, width=8)
}

args <- commandArgs(trailingOnly = TRUE)
mvid <- args[1]
outdir <- args[2]
indir <- args[3]
year_id <- strtoi(args[4])
for (location_id in c(1, 4, 5, 6, 492)){
    if (location_id==1) {
        s <- "both"
        y <- 2000
    } else {
        s <- "male"
        y <- year_id
    }
    plot_effects(mvid, location_id, y, s, indir, outdir)
}
