#' Make InfluxDB query
#'
#' @param q query
#' @param con an InfluxDB connection object
#' @param db database
#' @param epoch unit of epoch
#' @param httr_config additional httr curl config passed to httr::POST()
#'
#' @return a data.table
#' @export
#'
influxdb_query <- function(q, con, db, epoch, httr_config = list()) {

  query <- list(pretty = "false")
  if (!missing(db)) {
    query$db <- db
  }
  if (!missing(epoch)) {
    query$epoch <- epoch
  }

  r <- influxdb_post(con, endpoint = "query", query = query, body = list(q = q),
                     httr_config = httr_config, parser = "csv")
  influxdb_csv(influxdb_chkr(r))
}



