#' @title Is this the new cluster?
#'
#' @description Returns logical indicating if program is running on the new cluster.
#'
#' @return logical indicating if program is on the new cluster.
#'
#' @export
is_new_cluster <- function() {
  if (Sys.getenv("SGE_ENV") == "" & Sys.getenv("RSTUDIO_HTTP_REFERER") == "") {
    stop("Neither SGE_ENV nor RSTUDIO_HTTP_REFERER are found. Unidentifiable environment, exiting.")
  }
  
  if (Sys.getenv("SGE_ENV") == "") {
    warning("SGE_ENV is not set")
    
    # NOTE: If this is RStudio, then SGE_ENV isn't inherited, hence
    # we check using the URL
    is_new_hack <- grepl("-uge-", Sys.getenv("RSTUDIO_HTTP_REFERER"))
    is_old_hack <- grepl("cn|c2", Sys.getenv("RSTUDIO_HTTP_REFERER"))
    if (is_new_hack) {
      warning("Running RStudio on the fair cluster")
      return(TRUE)
    } else if (is_old_hack) {
      warning("Using... the old cluster?")
      return(FALSE)
    } else {
      stop("URL does not either have 'uge' or start with 'cn' or 'c2'")
    }
  } else {
    # per @matpp in #new-cluster-rollout at 9:02PM on 25 October 2018
    return(grepl("-el7$", Sys.getenv("SGE_ENV")))
  }
}