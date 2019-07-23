#' Convert data to InfluxDB line protocol
#'
#' @details This function checks whether measurement/tag/field/time are named.
#'  If not, these arguments are used as references to columns in data. Checking
#'  and escaping special characters are turned off by default to improve
#'  performance. If you have special characters in your data, set do_escape = TRUE
#'
#' @param data a named list-like object
#' @param measurement name of measurement or column name of measurement in data
#' @param tag a named list of tag k-v, or column name of tags in data
#' @param field a named list of field k-v, or column name of fields in data
#' @param time a vector of POSIXct, or column name of time in data
#' @param do_escape whether to check special characters in key/value that needs to escape
#' @param force_integer whether to use integer data type
#' @param epoch unit of epoch a.k.a precision
#'
#' @return a vector of characters
#' @export
#'
line_protocol <- function(data = list(), measurement, tag = NULL, field = NULL,
                          time = NULL, do_escape = FALSE, force_integer = FALSE,
                          epoch = c("ns", "u", "ms", "s", "m", "h")) {

  #Fallback to list
  class(data) <- NULL
  cols <- names(data)

  chk_NA <- any(sapply(data, anyNA))

  m_col <- NULL
  if (any(measurement == cols)) {
    m_col <- measurement
    measurement <- data[[m_col]]
  }
  if (do_escape) {
    measurement <- list(escape(measurement, esc_equal = FALSE))
  } else {
    measurement <- list(measurement)
  }

  tag_col <- NULL
  if (is.null(tag)) {
    tag_kv <- " "
  } else if (is.null(names(tag))) {
    #tag is passed as ref to column
    tag_col <- tag
    tag_kv <- c(",", convert_kv(data[tag_col], do_quote = FALSE, do_escape = do_escape,
                                force_integer = force_integer))
  } else {
    #tag is passed as named object
    tag_kv <- c(",", convert_kv(tag, do_quote = FALSE, do_escape = do_escape,
                                force_integer = force_integer))
  }

  epoch <- match.arg(epoch)

  time_col <- NULL
  if (is.null(time)) {
    time_v <- ""
  } else {
    if (is.character(time)) {
      #time is passed as ref to column
      time_col <- time
      time <- data[[time_col]]
    }
    time_v <- list(sprintf("%0.f", switch(epoch,
                                            ns = unclass(time) * 1.0e9,
                                            u  = unclass(time) * 1.0e6,
                                            ms = unclass(time) * 1.0e3,
                                            s  = unclass(time) * 1.0,
                                            m  = unclass(time) / 60.0,
                                            h  = unclass(time) / 3600.0)))
  }

  if (is.null(field)) {
    #use all fields left in data
    field_col <- setdiff(cols, c(m_col, tag_col, time_col))
    field_kv <- convert_kv(data[field_col], do_quote = TRUE, do_escape = do_escape,
                           force_integer = force_integer)
  } else if (is.null(names(field))) {
    #field is passed as ref to column
    field_col <- field
    field_kv <- convert_kv(data[field_col], do_quote = TRUE, do_escape = do_escape,
                           force_integer = force_integer)
  } else {
    #field is passed as named object
    field_kv <- convert_kv(field, do_quote = TRUE, do_escape = do_escape,
                           force_integer = force_integer)
  }

  ans <- do.call(paste0, c(measurement, tag_kv, field_kv, time_v))
  if (chk_NA) {
    na_field <- "((?<= )(?:(?!,|=).)*?=NA,)|(,(?:(?!,|=).)*?=NA)"
    ans <- stringr::str_replace_all(ans, na_field, "")
  } else {
    #do not print
    invisible(ans)
  }
}

#' Convert a named list to key-value pairs
#'
#' @param x a named list
#' @param do_quote whether to quote values
#' @param do_escape whether to check and escape special characters
#' @param force_integer wheter to use integer data type
#'
#' @return a list of key-value paris
convert_kv <- function(x, do_quote, do_escape, force_integer) {

  keys <- names(x)
  if (is.null(keys)) {
    stop("Converting unnamed list to key-value pairs.")
  }

  n <- 4L * length(x)
  ans <- vector(mode = "list", length = n)

  idx <- seq.int(from = 1L, to = n, by = 4L)

  #assign keys, offset 0
  ans[idx] <- if (do_escape) {
    escape(keys, esc_equal = TRUE)
  } else {
    keys
  }
  #assign equal signs, offset 1
  ans[idx + 1L] <- list("=")
  #assign values, offset 2
  ans[idx + 2L] <- if (do_quote) {
    lapply(x, quote, do_escape = do_escape, force_integer = force_integer)
  } else if (do_escape) {
    lapply(x, escape, esc_equal = TRUE)
  } else {
    x
  }
  #assign commas, offset 3
  ans[idx + 3L] <- list(",")

  #alter last element to a space character
  ans[[n]] <- " "

  ans
}

#' Check and escape special characters
#'
#' @param x a vector of characters
#' @param esc_equal whether to escape equal signs
#'
#' @return a vector of characters
escape <- function(x, esc_equal) {

  #escape commas
  x <- stringr::str_replace_all(x, stringr::fixed(","), "\\,")
  #escape spaces
  x <- stringr::str_replace_all(x, stringr::fixed(" "), "\\ ")

  if (esc_equal) {
    #escape equal signs
    x <- stringr::str_replace_all(x, stringr::fixed("="), "\\=")
  }

  x
}

#' Check and quote values if requied
#'
#' @param x a vector of values
#' @param do_escape whether to check and escape special characters
#' @param force_integer whether to use integer data type
#'
#' @return a vector of values
quote <- function(x, do_escape, force_integer) {

  if (is.numeric(x) || is.logical(x)) {
    if (force_integer && is.integer(x)) {
      return(sprintf("%di", x))
    } else {
      return(x)
    }
  }

  if (lubridate::is.Date(x) || lubridate::is.POSIXct(x)) {
    return(unclass(x))
  }

  if (do_escape) {
    if (is.character(x)) {
      #escape "
      x <- stringr::str_replace_all(x, stringr::fixed("\""), "\\\"")
      #escape \
      x <- stringr::str_replace_all(x, stringr::fixed("\\"), "\\\\")
      #quote
      x <- paste0("\"", x, "\"")
    } else if (is.factor(x)) {
      lvls <- levels(x)
      #escape "
      lvls <- stringr::str_replace_all(lvls, stringr::fixed("\""), "\\\"")
      #escape \
      lvls <- stringr::str_replace_all(lvls, stringr::fixed("\\"), "\\\\")
      #quote
      levels(x) <- paste0("\"", lvls, "\"")
    }
  } else {
    if (is.character(x)) {
      x <- paste0("\"", x, "\"")
    } else if (is.factor(x)) {
      lvls <- levels(x)
      levels(x) <- paste0("\"", lvls, "\"")
    }
  }

  x
}
