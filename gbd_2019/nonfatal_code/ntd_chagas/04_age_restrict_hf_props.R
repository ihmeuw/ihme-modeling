# Alter heart failure proportion for new age restriction
# data manipulate afib
#############################################################

run <- 'ADDRESS'

# hr proportion edits
pr_seq_draws <- as.data.table(read.dta13("FILEPATH")))

# extend proportion from age group id 20 onwards by sex
draw_m <- copy(pr_seq_draws[outcome == "hf" & age_group_id == 20 & sex_id == 1, paste0("draw_", 0:999)])
draw_f <- copy(pr_seq_draws[outcome == "hf" & age_group_id == 20 & sex_id == 2, paste0("draw_", 0:999)])

pr_seq_draws[outcome == "hf" & age_group_id > 20 & sex_id == 1, paste0("draw_", 0:999) := draw_m]
pr_seq_draws[outcome == "hf" & age_group_id > 20 & sex_id == 2, paste0("draw_", 0:999) := draw_f]

# age restrict (proportion 0) where age end is less than 30 years old
pr_seq_draws[outcome == "hf" & age_end <= 30, paste0("draw_", 0:999) := 0]

# edit afib to correct encoding
pr_seq_draws[sex_id == "Male", sex_id := "1"]
pr_seq_draws[sex_id == "Female", sex_id := "2"]

afib0 <- copy(pr_seq_draws[outcome == "" & age_group_id < 7])
afib0[, outcome := 'afib']
afib0[, modelable_entity_id := ADDRESS]
pr_seq_draws <- pr_seq_draws[outcome != ""]
pr_seq_draws <- rbind(pr_seq_draws, afib0)

# write out
save.dta13(pr_seq_draws, "FILEPATH"))
fwrite(pr_seq_draws, "FILEPATH"))