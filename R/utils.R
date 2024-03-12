#' @include zzz.R

`%||%` = function(x, y) if(is.null(x)) y else x

str_split <- function(string,
                      pattern,
                      n = Inf,
                      perl = FALSE,
                      fixed = FALSE,
                      useBytes = FALSE) {
  if (n == Inf) {
    strsplit(string, pattern, perl = perl, fixed = fixed, useBytes = useBytes)
  } else if (n == 1) {
    string
  } else {
    matches <- gregexpr(
      pattern, string, perl = perl, fixed = fixed, useBytes = useBytes
    )
    lapply(seq_along(matches), function(i) {
      match <- matches[[i]]
      char <- string[[i]]
      if (length(match) == 1 && match == -1) {
        return(char)
      } else {
        size <- seq_len(min(n - 1, length(match)))
        start <- c(1, match[size] + attr(match, "match.length")[size])
        end <- c(match[size] - 1, nchar(char))
      }
      substring(char, start, end)
    })
  }
}

path_abs <- function(path) {
  return(normalizePath(path, winslash = "/", mustWork = FALSE))
}

path_rel <- function(path, cwd = getwd()){
  rel_path = gsub(cwd, "", path)
  return(sub("^/+", "", rel_path, perl = TRUE))
}

is_uri <- function(path){
  startsWith(path, "s3://")
}

# get parent pkg function and method
pkg_method <- function(fun, pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(fun,' requires the ', pkg,' package, please install it first and try again',
         call. = F)}
  fun_name <- utils::getFromNamespace(fun, pkg)
  return(fun_name)
}

get_region <- function(...) {
  fun <- pkg_method("get_region", "paws.common")
  tryCatch({
    fun(...)
  }, error = function(e) {
    "us-east-1"
  })
}

split_vec <- function(vec, len, max_len = length(vec)){
  start <- seq(1, max_len, len)
  end <- c(start[-1]-1, max_len)
  lapply(seq_along(start), function(i) vec[start[i]:end[i]])
}

camel_to_snake = function(name) {
  name = gsub('(.)([A-Z][a-z]+)', '\\1_\\2', name)
  return(tolower(gsub('([a-z0-9])([A-Z])', '\\1_\\2', name)))
}

list_zip = function(...){
  kwargs = list(...)
  .mapply(list, kwargs, NULL)
}

now_utc = function(){
  now <- Sys.time()
  attr(now, "tzone") <- "UTC"
  now
}

na_posixct <- function() {
  out = NA_integer_
  class(out) <-  c("POSIXct", "POSIXt")
  return(out)
}

as.na <- function(x) {
  switch(
    class(x)[[1]],
    "logical" = NA,
    "character" = NA_character_,
    "integer" = NA_integer_,
    "POSIXct" = na_posixct(),
    x
  )
}
