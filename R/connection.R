#' Create an InfluxDB connection
#'
#' @param host hostname
#' @param port port number
#' @param user username
#' @param pass password
#' @param scheme scheme to use
#' @param path API path
#'
#' @return an InfluxDB connection
#' @export
#'
influxdb <- function(host, port = 8086, user = NULL, pass = NULL,
                     scheme = c("http", "https"), path = "/") {

  scheme <- match.arg(scheme)

  con <- list(
    scheme = scheme,
    host = host,
    port = port,
    user = user,
    pass = pass,
    path = path
  )

  structure(con, class = "influxdb")
}

#' Print an InfluxDB connection
#'
#' @param x an InfluxDB connection
#' @param ... not used
#'
#' @return x, invisibly
#' @export
#'
'print.influxdb' <- function(x, ...) {

  ping <- influxdb_ping(x)
  info <- sprintf("InfluxDB build %s version %s @ %s://%s:%d%s",
                  ping$build, ping$version, x$scheme, x$host, x$port, x$path)
  print(info)

  invisible(x)
}

#' Call InfluxDB HTTP API
#'
#' @param con an InfluxDB connection
#' @param func function to call
#'
#' @return parsed csv
#' @export
'$.influxdb' <- function(con, func) {

  if (func %in% names(con)) {
    return(con[[func]])
  }

  f <- function(q, ...) {

    if (missing(q)) {
      body <- list(...)
    } else {
      body <- c(list(q = q), list(...))
    }

    if (func == "ping") {
      r <- influxdb_get(con, endpoint = func, parser = "csv")
    } else {
      r <- influxdb_post(con, endpoint = func, body = body, parser = "csv")
    }

    influxdb_csv(
      influxdb_chkr(r)
    )
  }

  f
}

influxdb_credential <- function(con) {

  if (is.null(con$user)) {
    NULL
  } else {
    list(
      u = con$user,
      p = con$pass
    )
  }
}

influxdb_get <- function(con, endpoint, query = NULL, httr_config = list(),
                         parser = c("json", "csv")) {

  query <- c(influxdb_credential(con), query)

  parser <- match.arg(parser)
  header_accept <- switch(parser,
                          json = httr::accept_json(),
                          csv = httr::accept("text/csv"))

  r <- tryCatch({
    httr::GET(
      url = "",
      config = httr_config,
      #modify url
      scheme = con$scheme,
      hostname = con$host,
      port = con$port,
      path = sprintf("%s%s", con$path, endpoint),
      query = query,
      #default return data
      header_accept
    )
  }, error = base::identity)

  r
}

influxdb_post <- function(con, endpoint, query = NULL, body = NULL, httr_config = list(),
                          parser = c("json", "csv")) {

  query <- c(influxdb_credential(con), query)

  parser <- match.arg(parser)
  header_accept <- switch(parser,
                          json = httr::accept_json(),
                          csv = httr::accept("text/csv"))

  r <- tryCatch({
    httr::POST(
      url = "",
      config = httr_config,
      #modify url
      scheme = con$scheme,
      hostname = con$host,
      port = con$port,
      path = sprintf("%s%s", con$path, endpoint),
      query = query,
      #POST
      body = body,
      #default return data
      header_accept
    )
  }, error = base::identity)

  r
}

influxdb_chkr <- function(r) {

  if ("error" %in% class(r)) {
    msg <- sprintf("Connection error: %s", r$message)
    stop(msg)
  }
  if (!r$status_code %in% c(200, 204)) {
    if (grepl("csv", httr::http_type(r), fixed = TRUE)) {
      err <- httr::content(r, encoding = "UTF-8", col_types = "")
    } else {
      err <- httr::content(r, encoding = "UTF-8")
    }
    if (is.list(err)) {
      stop(err$error)
    } else {
      stop(err)
    }
  }

  r
}

influxdb_csv <- function(r) {

  if (globals$dt_avail) {
    text <- httr::content(r, as = "text", encoding = "UTF-8")
    if (text == "") {
      NULL
    } else {
      globals$dt_fread(text = text)
    }
  } else {
    httr::content(r, encoding = "UTF-8", col_types = "")
  }
}

#' InfluxDB ping
#'
#' @param con an InfluxDB connection object
#'
#' @return list containing InfluxDB build and version
#' @export
#'
influxdb_ping <- function(con) {

  r <- influxdb_get(con, "ping")

  header <- httr::headers(influxdb_chkr(r))
  list(
    build = header$`x-influxdb-build`,
    version = header$`x-influxdb-version`
  )
}
