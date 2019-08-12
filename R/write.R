#' Write data to InfluxDB
#'
#' @param data data to write, a vector line protocals or a named list-like object
#' @param con a InfluxDB connection
#' @param db database to write to
#' @param precision precision of the epoch
#' @param rp retention policy name
#' @param consistency consistency
#' @param httr_config additional httr curl config passed to httr::POST()
#' @param ... futher arguments passed to line_protocol()
#'
#' @return httr reponse
#' @export
#'
influxdb_write <- function(data, con, db, precision, rp, consistency,
                           httr_config = list(), ...) {
  UseMethod("influxdb_write")
}

#' @rdname influxdb_write
#' @export
influxdb_write.default <- function(data, con, db, precision, rp, consistency,
                                   httr_config = list(), ...) {

  if (missing(precision)) {
    lp <- line_protocol(data = data, ...)
  } else {
    lp <- line_protocol(data = data, epoch = precision, ...)
  }

  influxdb_write.character(data = lp, con = con, db = db, precision = precision,
                           rp = rp, consistency = consistency,
                           httr_config = httr_config)
}

#' @rdname influxdb_write
#' @export
influxdb_write.character <- function(data, con, db, precision, rp, consistency,
                                     httr_config = list(), ...) {

  query <- list(db = db)
  if (!missing(precision)) {
    query$precision <- precision
  }
  if (!missing(rp)) {
    query$rp <- rp
  }
  if (!missing(consistency)) {
    query$consistency <- consistency
  }

  #data_chunk <- split(data, seq_along(data) %/% chunk_size)

  r <- influxdb_post(con, endpoint = "write", query = query, body = data, httr_config = httr_config)
  influxdb_chkr(r)

  NULL
}
