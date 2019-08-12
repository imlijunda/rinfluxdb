#' @rdname line_protocol
#' @export
#'
line_protocol_gen.default <- function(ref, measurement, tag = NULL, field = NULL, time = NULL,
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
    m_fmt <- "%s"
  } else {
    #fixed m
    m_ref <- NULL
    m_fmt <- escape_m(measurement)
  }

  if (is.null(tag)) {
    m_fmt <- paste0(m_fmt, " ")
    tag_ref <- NULL
    tag_fmt <- ""
  } else {
    m_fmt <- paste0(m_fmt, ",")
    tag_ref <- sort(tag)
    tag_fmt <- kv_fmtstr(ref = ref, key = tag_ref, esc = escape_kv,
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
  f_fmt <- kv_fmtstr(ref = ref, key = f_ref, esc = escape_kv,
                     use_int = use_int, fp_prec = fp_prec, quote_str = TRUE, tail = f_tail)
  f_fmt_na <- paste0(",", f_fmt)

  fmt <- paste0(m_fmt, tag_fmt, f_fmt, time_fmt)
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

  handle_na <- function(data, idx) {

    token <- sprintf("@_=_N_A@%.10f", runif(n = 1))
    tokenised <- FALSE

    #insert token into string fields
    f_spf_args <- list()
    for (k in f_ref) {
      tmp <- esc_fv(data[[k]])
      if (is.character(tmp)) {
        tmp[idx] <- stringr::str_replace_all(tmp[idx], stringr::fixed("=NA"), token)
        tokenised <- TRUE
      }
      f_spf_args <- append(f_spf_args, list(tmp))
    }
    f_spf <- do.call(sprintf, c(f_fmt_na, f_spf_args))

    handle_na_str(str = f_spf, idx = idx, token = token, tokenised = tokenised)
  }

  f <- function(data) {
    data <- unclass(data)
    spf_args <- list()
    #measurement
    if (!is.null(m_ref)) {
      tmp <- esc_m(data[[m_ref]])
      spf_args <- append(spf_args, list(tmp))
    }
    #tag
    for (k in tag_ref) {
      tmp <- esc_kv(data[[k]])
      spf_args <- append(spf_args, list(tmp))
    }
    #field
    if (chk_na) {
      chk <- as.logical(rowSums(sapply(data[f_ref], is.na, USE.NAMES = FALSE)))
      if (any(chk)) {
        fv <- handle_na(data, idx = chk)
        spf_args <- append(spf_args, list(fv))
        fmt_to_use <- fmt_na
      } else {
        for (k in f_ref) {
          tmp <- esc_fv(data[[k]])
          spf_args <- append(spf_args, list(tmp))
        }
        fmt_to_use <- fmt
      }
    } else {
      for (k in f_ref) {
        tmp <- esc_fv(data[[k]])
        spf_args <- append(spf_args, list(tmp))
      }
      fmt_to_use <- fmt
    }
    #time
    if (!is.null(time_ref)) {
      tmp <- unclass(data[[time_ref]]) * epoch
      spf_args <- append(spf_args, list(tmp))
    }
    do.call(sprintf, args = c(fmt_to_use, spf_args))
  }

  f
}
