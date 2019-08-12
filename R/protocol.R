#' Convert data to InfluxDB line protocol
#'
#' @details line_protocol() converts a named data structure to a character vector
#' of InfluxDB line protocol. It is a wrapper of line_protocol_gen(), which parses
#' reference data and returns a converter function that accepts same data structure.
#'
#'  Remarks on NA values. if chk_na is TURE, line_protocol_gen() checks for NA
#'  values in fields ONLY and uses regex ",(?:(?!,|=).)*?=NA" to match/remove NA
#'  fields. However this comes with two caveat: "=NA" inside string field values
#'  are replace by random token @_=_N_A@ + a random number and later replaced back
#'  to =NA, if you have such values and accidentally with same random token,
#'  Secondly, as you may notice, the mentioned regex can't distinguish field
#'  keys with special characters such as special\,key\=NA=100 will cause problem.
#'
#' @param ref a reference data structure
#' @param data data to convert.
#' @param measurement measurement name, or column name of measurement in ref
#' @param tag column names of tags in ref
#' @param field column names of fields in ref, if NULL, all columns will be used as fields
#' @param time column name of time in ref
#' @param chk_escape whether to check special characters to escape
#' @param chk_na whether to check NA values
#' @param use_int whether to use integer data type
#' @param fp_prec precision of floating point data, if NULL, system default is used
#' @param epoch unit of epoch
#'
#' @return a function that accepts same data structure as ref, or a character vector of line protocol
#' @export
#'
#' @examples
#' ref <- iris
#' f <- line_protocol_gen(ref, "iris", tag = "Species")
#' lp <- f(ref)
#' lp2 <- line_protocol(ref, "iris", tag = "Species")
#' stopifnot(all(lp == lp2))
line_protocol <- function(data, measurement, tag = NULL, field = NULL, time = NULL,
                          chk_escape = FALSE, chk_na = FALSE, use_int = FALSE, fp_prec = NULL,
                          epoch = c("ns", "u", "ms", "s", "m", "h")) {

  f <- line_protocol_gen(data, measurement, tag = tag, field = field, time = time,
                         chk_escape = chk_escape, chk_na = chk_na, use_int = use_int, fp_prec = fp_prec,
                         epoch = epoch)
  f(data)
}

#' @rdname line_protocol
#' @export
line_protocol_gen <- function(ref, measurement, tag = NULL, field = NULL, time = NULL,
                              chk_escape = FALSE, chk_na = FALSE, use_int = FALSE, fp_prec = NULL,
                              epoch = c("ns", "u", "ms", "s", "m", "h")) {

  UseMethod("line_protocol_gen", ref)
}

handle_na_str <- function(str, idx, token, tokenised) {

  #look for NA fields
  na_field <- ",(?:(?!,|=).)*?=NA"
  #remove =NA
  str[idx] <- stringr::str_remove_all(str[idx], na_field)
  if (tokenised) {
    #put back =NA in string values
    str[idx] <- stringr::str_replace_all(str[idx], stringr::fixed(token), "=NA")
  }
  #remove leading comma
  stringr::str_sub(str, start = 2L)
}

#' Generate format string for key=value pair from a reference named list.
#'
#' @param ref a named list, or a matrix-like object
#' @param key key
#' @param esc a function to escape special characters in key
#' @param use_int whether to use integer data type
#' @param fp_prec fp precision
#' @param quote_str whether to quote string value
#' @param as_str whether to print all values as string without quote
#' @param op operator
#' @param sep seperator
#' @param tail tailing string
#'
#' @return a character vector
kv_fmtstr <- function(ref, key, esc,
                      use_int = FALSE, fp_prec = NULL, quote_str = TRUE, as_str = FALSE,
                      op = "=", sep = ",", tail = " ") {

  if (is.null(key)) {
    return(tail)
  }

  n <- 3L * length(key)
  idx <- seq.int(from = 1L, to = n, by = 3L)
  ans <- vector(mode = "character", length = n)

  ans[idx] <- paste0(esc(key), op)
  ans[idx + 2L] <- sep
  ans[n] <- tail

  if (as_str) {
    ans[idx + 1L] <- "%s"
  } else {
    idx <- idx + 1L

    dbl <- if (is.null(fp_prec)) "%f" else paste0("%.", fp_prec, "f")
    int <- if (use_int) "%di" else "%d"
    chr <- if (quote_str) '"%s"' else "%s"

    if (is.matrix(ref)) {
      ans[idx] <- switch(typeof(ref),
                         double = dbl,
                         integer = int,
                         character = chr,
                         "%s")
    } else {
      type <- sapply(ref[key], typeof, USE.NAMES = FALSE)
      for (i in seq_along(key)) {
        ans[idx[i]] <- switch(type[i],
                              double = dbl,
                              integer = int,
                              character = chr,
                              "%s")
      }
    }
  }

  paste0(ans, collapse = "")
}

escape_m <- function(x) {

  #escape commas
  x <- stringr::str_replace_all(x, stringr::fixed(","), "\\,")
  #escape spaces
  x <- stringr::str_replace_all(x, stringr::fixed(" "), "\\ ")

  x
}

escape_kv <- function(x) {

  #escape commas
  x <- stringr::str_replace_all(x, stringr::fixed(","), "\\,")
  #escape spaces
  x <- stringr::str_replace_all(x, stringr::fixed(" "), "\\ ")
  #escape equals
  x <- stringr::str_replace_all(x, stringr::fixed("="), "\\=")

  x
}

escape_fv <- function(x) {

  if (is.numeric(x)) {
    return(x)
  }

  if (lubridate::is.Date(x) || lubridate::is.POSIXct(x)) {
    return(unclass(x))
  }

  if (is.character(x)) {
    x <- stringr::str_replace_all(x, stringr::fixed("\""), "\\\"")
    x <- stringr::str_replace_all(x, stringr::fixed("\\"), "\\\\")
    return(x)
  }

  if (is.factor(x)) {
    lvls <- levels(x)
    lvls <- stringr::str_replace_all(lvls, stringr::fixed("\""), "\\\"")
    lvls <- stringr::str_replace_all(lvls, stringr::fixed("\\"), "\\\\")
    levels(x) <- lvls
    return(x)
  }

  x
}
