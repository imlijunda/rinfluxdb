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
#'
line_protocol_gen <- function(ref, measurement, tag = NULL, field = NULL, time = NULL,
                              chk_escape = FALSE, chk_na = FALSE, use_int = FALSE, fp_prec = NULL,
                              epoch = c("ns", "u", "ms", "s", "m", "h")) {

  ref <- unclass(ref)
  if (!length(ref)) {
    stop("No reference data provided.")
  }

  key <- names(ref)
  if (is.null(key)) {
    stop("Reference data must be named.")
  }

  if (length(measurement) != 1L) {
    stop("Length of measurement must be 1.")
  }

  if (measurement %in% key) {
    m_ref <- measurement
    m <- NULL
  } else {
    m_ref <- NULL
    m <- escape_m(measurement)
  }

  if (is.null(tag)) {
    m_fmt <- "%s "
    tag_ref <- NULL
    tag_key <- NULL
    tag_fmt <- ""
  } else {
    m_fmt <- "%s,"
    tag_ref <- sort(tag)
    tag_key <- escape_kv(tag_ref)
    tag_fmt <- kv_fmtstr(ref = ref, key = tag_ref,
                         use_int = use_int, fp_prec = fp_prec, quote_str = FALSE, as_str = TRUE)
  }

  epoch <- match.arg(epoch)
  epoch <- switch(epoch,
                  ns = 1.0e9,
                  u  = 1.0e6,
                  ms = 1.0e3,
                  s  = 1.0,
                  m  = 1.0 / 60,
                  h  = 1.0 / 3600)
  if (is.null(time)) {
    time_ref <- NULL
    time_fmt <- ""
    f_tail <- ""
  } else {
    time_ref <- time
    time_fmt <- "%.0f"
    f_tail <- " "
  }

  if (is.null(field)) {
    f_ref <- setdiff(key, c(m_ref, tag_ref, time_ref))
  } else {
    f_ref <- field
  }
  f_key <- escape_kv(f_ref)
  f_fmt <- kv_fmtstr(ref = ref, key = f_ref,
                     use_int = use_int, fp_prec = fp_prec, quote_str = TRUE, tail = f_tail)

  fmt    <- paste0(m_fmt, tag_fmt, f_fmt, time_fmt)

  #update f_fmt in case of chk_na
  f_fmt <- paste0(",", f_fmt)
  fmt_na <- paste0(m_fmt, tag_fmt, "%s",  time_fmt)

  if (chk_escape) {
    esc_m <- escape_m
    esc_kv <- escape_kv
  } else {
    esc_m <- base::identity
    esc_kv <- base::identity
  }
  esc_fv <- escape_fv

  #up to this point, ref is not needed anymore, remove it in case of large ref passed.
  rm(list = c("ref"))

  f <- function(data) {

    data <- unclass(data)

    spf_args <- list()

    #measurement
    if (is.null(m_ref)) {
      spf_args <- append(spf_args, m)
    } else {
      tmp <- esc_m(data[[m_ref]])
      spf_args <- append(spf_args, list(tmp))
    }

    #tag
    for (i in seq_along(tag_ref)) {
      spf_args <- append(spf_args, tag_key[i])
      tmp <- esc_kv(data[[tag_ref[i]]])
      spf_args <- append(spf_args, list(tmp))
    }

    #field
    for (i in seq_along(f_ref)) {
      spf_args <- append(spf_args, f_key[i])
      tmp <- esc_fv(data[[f_ref[i]]])
      spf_args <- append(spf_args, list(tmp))
    }

    #time
    if (!is.null(time_ref)) {
      tmp <- unclass(data[[time_ref]]) * epoch
      spf_args <- append(spf_args, list(tmp))
    }

    do.call(sprintf, args = c(fmt, spf_args))
  }

  if (chk_na) {

    f_na <- function(data) {

      data <- unclass(data)

      #check for NA values
      chk <- as.logical(rowSums(sapply(data[f_ref], is.na, USE.NAMES = FALSE)))
      if (!any(chk)) {
        #fallback to normal method for performance
        return(f(data))
      }

      token <- sprintf("@_=_N_A@%.10f", runif(n = 1))
      #insert token into string fields
      f_spf_args <- list()
      for (i in seq_along(f_ref)) {
        f_spf_args <- append(f_spf_args, f_key[i])
        tmp <- esc_fv(data[[f_ref[i]]])
        if (is.character(tmp)) {
          tmp[chk] <- stringr::str_replace_all(tmp[chk], stringr::fixed("=NA"), token)
        }
        f_spf_args <- append(f_spf_args, list(tmp))
      }
      f_spf <- do.call(sprintf, c(f_fmt, f_spf_args))
      #look for NA fields
      na_field <- ",(?:(?!,|=).)*?=NA"
      #remove =NA
      f_spf[chk] <- stringr::str_remove_all(f_spf[chk], na_field)
      #put back =NA in string values
      f_spf[chk] <- stringr::str_replace_all(f_spf[chk], stringr::fixed(token), "=NA")
      #remove leading comma
      f_spf <- stringr::str_sub(f_spf, start = 2L)

      #up to this point, the remaining is same as f()
      spf_args <- list()

      #measurement
      if (is.null(m_ref)) {
        spf_args <- append(spf_args, m)
      } else {
        tmp <- esc_m(data[[m_ref]])
        spf_args <- append(spf_args, list(tmp))
      }

      #tag
      for (i in seq_along(tag_ref)) {
        spf_args <- append(spf_args, tag_key[i])
        tmp <- esc_kv(data[[tag_ref[i]]])
        spf_args <- append(spf_args, list(tmp))
      }

      #field
      spf_args <- append(spf_args, list(f_spf))

      #time
      if (!is.null(time_ref)) {
        tmp <- unclass(data[[time_ref]]) * epoch
        spf_args <- append(spf_args, list(tmp))
      }

      do.call(sprintf, args = c(fmt_na, spf_args))
    }

    return(f_na)
  }

  f
}

#' Generate format string for key=value pair from a reference named list.
#'
#' @param ref a named list
#' @param key key
#' @param use_int whether to use integer data type
#' @param fp_prec fp precision
#' @param quote_str whether to quote string value
#' @param as_str whether to print all values as string without quote
#' @param op operator
#' @param sep seperator
#' @param tail tailing string
#'
#' @return a character vector
kv_fmtstr <- function(ref, key, use_int, fp_prec, quote_str, as_str = FALSE,
                      op = "=", sep = ",", tail = " ") {

  if (is.null(key)) {
    return(tail)
  }

  n <- 3L * length(key)
  idx <- seq.int(from = 1L, to = n, by = 3L)
  ans <- vector(mode = "character", length = n)

  ans[idx] <- paste0("%s", op)
  ans[idx + 2L] <- sep
  ans[n] <- tail

  if (as_str) {
    ans[idx + 1L] <- "%s"
  } else {
    idx <- idx + 1L
    type <- sapply(ref[key], typeof, USE.NAMES = FALSE)

    dbl <- if (is.null(fp_prec)) "%f" else paste0("%.", fp_prec, "f")
    int <- if (use_int) "%di" else "%d"
    chr <- if (quote_str) '"%s"' else "%s"

    for (i in seq_along(key)) {
      ans[idx[i]] <- switch(type[i],
                            double = dbl,
                            integer = int,
                            character = chr,
                            "%s")
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
