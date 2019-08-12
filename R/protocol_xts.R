#' @rdname line_protocol
#' @export
line_protocol_gen.xts <- function(ref, measurement, tag = NULL, field = NULL, time = NULL,
                                  chk_escape = FALSE, chk_na = FALSE, use_int = FALSE, fp_prec = NULL,
                                  epoch = c("ns", "u", "ms", "s", "m", "h")) {

  if (length(measurement) != 1L) {
    stop("Length of measurement must be 1.")
  }

  m_fmt <- escape_m(measurement)

  if (is.null(field)) {
    f_ref <- colnames(ref)
  } else {
    f_ref <- field
  }
  f_fmt <- kv_fmtstr(ref = ref, key = f_ref, esc = escape_kv,
                     use_int = use_int, fp_prec = fp_prec, quote_str = TRUE)
  f_fmt_na <- paste0(",", f_fmt)

  epoch <- match.arg(epoch)
  epoch <- switch(epoch,
                  ns = 1.0e9,
                  u  = 1.0e6,
                  ms = 1.0e3,
                  s  = 1.0,
                  m  = 1.0 / 60,
                  h  = 1.0 / 3600)
  time_fmt <- "%.0f"

  get_tag_fmt <- function(ref, key) {
    kv_fmtstr(ref, key, esc = escape_kv,
              use_int = use_int, fp_prec = fp_prec, quote_str = FALSE, as_str = TRUE)
  }

  if (chk_escape) {
    esc_kv <- escape_kv
  } else {
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
    if (is.character(data)) {
      data[idx, ] <- stringr::str_replace_all(data[idx, ], stringr::fixed("=NA"), token)
      tokenised <- TRUE
    }
    f_spf_args <- lapply(f_ref, function(col) data[, col])
    f_spf <- do.call(sprintf, c(f_fmt_na, f_spf_args))

    handle_na_str(str = f_spf, idx = idx, token = token, tokenised = tokenised)
  }

  f <- function(data) {
    spf_args <- list()
    #tags
    attrs <- xts::xtsAttributes(data, user = TRUE)
    if (length(attrs)) {
      m_fmt <- paste0(m_fmt, ",")
      if (is.null(tag)) {
        tag_ref <- sort(names(attrs))
      } else {
        tag_ref <- tag
      }
      tag_fmt <- get_tag_fmt(attrs, tag_ref)
      for (k in tag_ref) {
        tmp <- esc_kv(attrs[[k]])
        spf_args <- append(spf_args, tmp)
      }
    } else {
      m_fmt <- paste0(m_fmt, " ")
      tag_fmt <- ""
    }
    #fields
    if (chk_na) {
      #unclass data to avoid creating extra xts objects
      tmp <- esc_fv(unclass(data))
      chk <- as.logical(rowSums(is.na(tmp)))
      if (any(chk)) {
        fv <- handle_na(tmp, idx = chk)
        spf_args <- append(spf_args, list(fv))
        fmt_to_use <- paste0(m_fmt, tag_fmt, "%s", time_fmt)
      } else {
        spf_args <- append(spf_args, lapply(f_ref, function(col) tmp[, col]))
        fmt_to_use <- paste0(m_fmt, tag_fmt, f_fmt, time_fmt)
      }
    } else {
      spf_args <- append(spf_args, lapply(f_ref, function(col) tmp[, col]))
      fmt_to_use <- paste0(m_fmt, tag_fmt, f_fmt, time_fmt)
    }
    #time
    time <- zoo::index(data)
    if (!lubridate::is.POSIXct(time)) {
      time <- as.POSIXct(time)
    }
    time <- unclass(time) * epoch
    spf_args <- append(spf_args, list(time))
    #compose line protocol
    do.call(sprintf, args = c(fmt_to_use, spf_args))
  }

  f
}
