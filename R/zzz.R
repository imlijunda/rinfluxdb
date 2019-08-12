#' @import httr
#' @importFrom lubridate is.Date is.POSIXct
#' @importFrom stringr str_replace_all fixed
#' @importFrom stats runif
#'
globals <- new.env()
globals$dt_avail <- FALSE
globals$dt_fread <- NULL

.onLoad <- function(libname, pkgname) {

  globals$dt_avail = base::requireNamespace("data.table", quietly = TRUE)
  if (globals$dt_avail) {
    globals$dt_fread <- utils::getFromNamespace("fread", "data.table")
  }
}
