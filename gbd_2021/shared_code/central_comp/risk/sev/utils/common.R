# Common utility functions for the SEV calculator

#' @param df the data.table that potentially contains duplicates
#' @param by_cols the columns to check for duplication
#'
#' @return a data.table with duplicates removed
dataframe_unique <- function(df, by_cols = NULL) {
    if (is.null(by_cols)) by_cols <- names(df)
    df <- as.data.frame(df)
    df$dupe <- duplicated(df[, by_cols], fromLast = FALSE)
    df <- subset(df, dupe == FALSE)
    df$dupe <- NULL
    return(as.data.table(df))
}

#' Downsample a data.table using the same method get_draws uses from ihme_dimensions.dfutils.
#' Will not upsample. Expects dfutils already loaded using reticulate.
#'
#' @param dt the data table to downsample
#' @param n_draws the number of draw columns to downsample to
#' @param df_utils the python module loaded using reticulate
#' @param draw_prefix the prefix used for all the numbered draw columns. Default is "draw_"
#'
#' @return data.table with draw columns reduced to the requested number
downsample <- function(dt, n_draws, df_utils, draw_prefix = "draw_") {
    dt_n_draws <- sum(names(dt) %like% draw_prefix)
    if (n_draws == dt_n_draws) return(dt)
    if (n_draws > dt_n_draws)
        stop("Unable to resample to ", n_draws, " draws: The dt has only ", dt_n_draws,
             " draw columns and upsampling is not allowed")

    return(as.data.table(df_utils$resample(dt, n_draws = as.integer(n_draws),
           draw_prefix = draw_prefix)))
}
