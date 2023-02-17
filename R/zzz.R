#' @import lgr
"_PACKAGE"

.onLoad <- function(libname, pkgname) {
  # set package logger
  assign(
    "LOGGER",
    lgr::get_logger(name = "s3fs"),
    envir = parent.env(environment())
  )
}
