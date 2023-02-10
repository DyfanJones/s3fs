#' @import lgr
"_PACKAGE"

.onLoad <- function(libname, pkgname) {
  # set package logs and don't propagate root logs
  .logger = lgr::get_logger(name = "s3fs")$set_propagate(FALSE)

  # set package logger
  assign(
    "LOGGER",
    .logger,
    envir = parent.env(environment())
  )
}
