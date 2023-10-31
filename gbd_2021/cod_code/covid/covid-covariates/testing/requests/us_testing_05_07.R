library(data.table)
library(ggplot2)
dt <- fread("FILEPATH/us_testing_05_07.csv")
dt[, date := as.Date(date, format = "%d.%m.%y")]
sum_dt <- dt[order(date), .(total = sum(total)), by = date]
sum_dt[, daily_total := total - shift(total, 1)]
sum_dt
plot_dt <- sum_dt[date >= "2020-03-01"]
pdf("FILEPATH/us_testing_05_07.pdf", width = 8, height = 6)
with(plot_dt, {
  plot(date, daily_total / 1e3,
    pch = 20,
    xlab = "Date", ylab = "Daily COVID-19 tests (in thousands)",
    main = "COVID-19 Testing in the United States"
  )
  par(adj = 0)
  title(
    sub = "Source: The COVID Tracking Project",
    cex.sub = 0.8
  )
})
dev.off()
