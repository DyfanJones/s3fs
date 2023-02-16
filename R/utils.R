#' @include zzz.R

`%||%` = function(x, y) if(is.null(x)) y else x


################################################################################
# Developed from:
# https://github.com/tidyverse/stringr/blob/2a30b2ebc8e854e181db0ef39025a9cbd35b75cf/R/split.r
# See the NOTICE file at the top of this package for attribution.
################################################################################
str_split <- function(string, pattern, n = Inf) {
  if (n == Inf) {
    strsplit(string, pattern)
  } else if (n == 1) {
    string
  } else {
    locations <- str_locate_all(string, pattern)
    lapply(locations, function(mat) {
      cut <- mat[seq_len(min(n - 1, nrow(mat))), , drop = FALSE]
      keep <- matrix(c(0, t(cut), Inf), ncol = 2, byrow = TRUE)

      str_sub(string, keep[, 1] + 1, keep[, 2] - 1)
    })
  }
}

str_locate_all <- function(string, pattern) {
  matches <- gregexpr(pattern, string)

  null <- matrix(0, nrow = 0, ncol = 2)
  colnames(null) <- c("start", "end")

  lapply(matches, function(match) {
    if (length(match) == 1 && match == -1) return(null)

    start <- as.vector(match)
    end <- start + attr(match, "match.length") - 1
    cbind(start = start, end = end)
  })
}

str_sub <- function(string, start = 0, end = Inf) {
  if (length(string) == 0 || length(start) == 0 || length(end) == 0) {
    return(vector("character", 0))
  }

  n <- max(length(string), length(start), length(end))
  string <- rep(string, length = n)
  start <- rep(start, length = n)
  end <- rep(end, length = n)

  # Replace infinite ends with length of string
  max_length <- !is.na(end) & end == Inf
  end[max_length] <- nchar(string)[max_length]

  substring(string, start, end)
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
