#' @author
#' @date 2020/06/15
#' @description Remove or offset zeros in a dataset
#' @function rm_zeros
#' @param dt bundle data used to be sex split
#' @param fix_ones Logical of whether to fix ones.  If F, only fix zeros.  If T, fix ones also
#' @param drop_zeros Logical of whether to drop zeros (and ones if applicable)
#' @param offset Logical of whether to offset zeros (and ones if applicable) with a % of the median
#' @return dt Data table with zeros (and ones if applicable) fixed
#' @note This needs to be run because MR-BRT cannot handle zeros (in log space)
#' and cannot handle zeros AND ones (in logit space)
#' This is how you decide whether fix_ones should be T (T if logit space, F if log space)


## Offsetting instances where mean = 0 or mean = 1 by 1% of median of non-zero values (like Dismod).
rm_zeros <- function(dt, 
                     offset = T, 
                     fix_ones = T, 
                     drop_zeros = F, 
                     quiet = F){
  if (offset) {
    off <- .01*median(dt[mean != 0, mean])
    if (nrow(dt[mean==0])>0) {
      if (!quiet) message("You have ", nrow(dt[mean==0])," zeros in your dataset that are being offset by 1% of the median of all non-zero values. To avoid this, set offset=F and drop_zeros=T.")
      dt[mean==0, mean := off]
    }
    if (fix_ones){
      if (nrow(dt[mean==1])>0){
        if (!quiet) message("You have ", nrow(dt[mean==1])," ones in your dataset that are being offset by 1% of the median of all non-zero values. To avoid this, set offset=F and drop_zeros=T.")
        dt[mean==1, mean := 1-off]
      }
    }
  }
  
  if (drop_zeros) {
    if (fix_ones){
      if (!quiet) message("Dropping ", nrow(dt[mean==1])," ones in your dataset.")
      dt <- dt[!mean == 1]
    }
      if (!quiet) message("Dropping ", nrow(dt[mean==0])," zeros in your dataset.")
      dt <- dt[!mean == 0]
  }
  return(dt)
}

