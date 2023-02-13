#' @include zzz.R

`%||%` = function(x, y) if(is.null(x)) y else x

str_split = function(path, pattern, n=-1L){
  out = strsplit(path, pattern)
  lapply(out, function(x) {
    if(n == -1L){
      return(x)
    } else {
      str_n = paste(x[n:length(x)], collapse = pattern)
      if (n == 1)
        return(str_n)
      return(c(x[1:(n - 1)], str_n))
    }
  })
}

path_abs <- function(path) {
  return(normalizePath(path, winslash = "/", mustWork = FALSE))
}

path_rel <- function(path, cwd = getwd()){
  rel_path = gsub(cwd, "", path)
  return(trimws(rel_path, "left", "/"))
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
