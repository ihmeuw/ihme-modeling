split_groups <- function(dt) {
  to_split <- copy(dt)[specificity != '', ]
  no_split <- copy(dt)[specificity == '', ]
  
  to_split[is.na(mean), mean := cases / sample_size]
  to_split[is.na(sample_size), sample_size := cases / mean]
  
  to_split[, split_group := .GRP, by = .(nid, measure, group)]
  to_split[, merge := 1] # we'll use this for cartesian merges below
  to_split[, group_review := 0]
  
  apply_group_specificity_splits <- function(sg) {
    counts <- table(to_split[split_group == sg, specificity])
    base_specificity <- names(which(counts == max(counts)))
    
    split_tmp <- copy(to_split)[split_group == sg & 
                                  specificity == base_specificity, ]
    
    for (spec in setdiff(names(counts), base_specificity)) {
      split_tmp_level <- copy(to_split)[split_group == sg & specificity == spec, ]
      
      split_tmp_level[, specificity_rr := mean / mean(mean)]
      
      if (anyNA(split_tmp_level$sample_size) == FALSE) {
        split_tmp_level[, pr_sample := sample_size / sum(sample_size)]
      } else {
        split_tmp_level[, pr_sample := 1 / .N]
      }
      
      specificity_vars <- grep(paste0('^', spec), names(split_tmp_level), value = T)
      keep_vars <- c(specificity_vars, 'specificity_rr', 'pr_sample', 'merge')
      
      split_tmp_level <- split_tmp_level[, .SD, .SDcols = keep_vars]
      split_tmp[, (specificity_vars) := NULL]
      
      split_tmp <- merge(split_tmp, split_tmp_level, by = 'merge', 
                         allow.cartesian = T, all = T)
    
      split_tmp[, mean := mean * specificity_rr]
      split_tmp[!is.na(sample_size), sample_size := sample_size * pr_sample]
      split_tmp[!is.na(effective_sample_size), 
                effective_sample_size := effective_sample_size * pr_sample]
      split_tmp[!is.na(cases), cases := mean * sample_size]
      
      split_tmp[!is.na(standard_error), 
                standard_error := sqrt(standard_error^2 / pr_sample)]
      split_tmp[!is.na(lower), lower := mean - qnorm(0.975) * 
                  sqrt(((mean - lower) / qnorm(0.975))^2 / pr_sample)]
      split_tmp[!is.na(upper), upper := mean + qnorm(0.975) * 
                  sqrt(((upper - mean) / qnorm(0.975))^2 / pr_sample)]
      
      split_tmp[, specificity := paste(specificity, spec, sep = ', ')]
      
      split_tmp[, c('specificity_rr', 'pr_sample') := NULL]
    }
    
    split_tmp[, group_review := 1]
    split_tmp[, seq := NULL]
    split_tmp[!is.na(lower) & !is.na(upper), uncertainty_type_value := 95]
    return(split_tmp)  
  }
  
  split <- rbindlist(lapply(unique(to_split$split_group), apply_group_specificity_splits))
  out <- rbind(split, to_split, no_split, fill = T)[, c('merge', 'split_group') := NULL]
  
  return(out)
  }
  
