# Make a simplified plot to compare two DDM versions

library(data.table)
library(ggplot2)

new_version <- x
old_version <- x

new_dir <- glue::glue("FILEPATH") # latest GBD 2020
old_dir <- glue::glue("FILEPATH") # best GBD 2019

## completeness by method and final completeness
new_data <- data.table(
  haven::read_dta(
    paste0("FILEPATH"),
    encoding='latin1'
  )
)
new_final <- data.table(
  haven::read_dta(
    paste0("FILEPATH")
  )
)

old_data <- data.table(
  haven::read_dta(
    paste0("FILEPATH"),
    encoding='latin1'
  )
)
old_final <- data.table(
  haven::read_dta(
    paste0("FILEPATH")
  )
)

data_keep_vars <- c(
  "ihme_loc_id", "iso3_sex_source", "year",
  "u5_comp", "comp_type", "comp",
  "pred1", "pred2", "exclude"
)

new_data <- new_data[, ..data_keep_vars]
old_data <- old_data[, ..data_keep_vars]

final_keep_vars <- c("source", "ihme_loc_id", "year", "sex", "u5_comp_pred", "final_comp")

new_final <- new_final[, ..final_keep_vars]
old_final <- old_final[, ..final_keep_vars]

new_final[, iso3_sex_source := paste0(ihme_loc_id, "&&", sex, "&&", source)]
old_final[, iso3_sex_source := paste0(ihme_loc_id, "&&", sex, "&&", source)]

manual_shapes <- c(1, 0, 15, 8, 16, 3)

names(manual_shapes) <- c("", "CCMP_aplus_migration", "CCMP_aplus_no_migration", "ggb", "ggbseg", "seg")

pdf(glue::glue("FILEPATH"))

for(iss in unique(new_data$iso3_sex_source)) {

  new_temp_data <- new_data[iso3_sex_source == iss]
  new_temp_final <- new_final[iso3_sex_source == iss]
  old_temp_data <- old_data[iso3_sex_source == iss]
  old_temp_final <- old_final[iso3_sex_source == iss]

p <- ggplot(
  data = new_temp_final,
  aes(x = year, y = comp)
) +
  # 5q0 data
  geom_point(
    data = new_temp_data[exclude == 0],
    aes(x = year, y = u5_comp),
    color = "red"
  ) +
  geom_point(
    data = old_temp_data[exclude == 0],
    aes(x = year, y = u5_comp),
    color = "red",
    alpha = 0.1
  ) +
  # 5q0 model
  geom_line(
    data = new_temp_final,
    aes(x = year, y = u5_comp_pred),
    color = "red"
  ) +
  geom_line(
    data = old_temp_final,
    aes(x = year, y = u5_comp_pred),
    color = "red",
    alpha = 0.3
  ) +
  # DDM data
  geom_point(
    data = new_temp_data[exclude == 0],
    aes(x = year, y = comp, shape = comp_type)
  ) +
  geom_point(
    data = old_temp_data[exclude == 0],
    aes(x = year, y = comp, shape = comp_type),
    alpha = 0.25
  ) +
  # DDM stage 1
  geom_line(
    data = new_temp_data,
    aes(x = year, y = pred1),
    linetype = "dotted"
  ) +
  geom_line(
    data = old_temp_data,
    aes(x = year, y = pred1),
    linetype = "dotted",
    alpha = 0.3
  ) +
  # DDM stage 2
  geom_line(
    data = new_temp_data,
    aes(x = year, y = pred2),
    linetype = "dashed"
  ) +
  geom_line(
    data = old_temp_data,
    aes(x = year, y = pred2),
    linetype = "dashed",
    alpha = 0.3
  ) +
  # DDM final
  geom_line(
    data = new_temp_final,
    aes(x = year, y = final_comp),
  ) +
  geom_line(
    data = old_temp_final,
    aes(x = year, y = final_comp),
    alpha = 0.3
  ) +
  # settings
  scale_shape_manual(values = manual_shapes) +
  theme_bw() +
  xlab("Year") +
  ylab("DDM") +
  ylim(0, 1.5) +
  ggtitle("DDM comparison for ", iss) +
  labs(caption = "Faded values are previous round best. Excludes outliers.") +
  theme(legend.position = "bottom")

print(p)

}

dev.off()
