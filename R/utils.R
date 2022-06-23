#' @include zzz.R

`%||%` = function(x, y) if(is.null(x)) y else x

str_split = function(path, pattern, n=-1L){
  out = strsplit(path, pattern)
  lapply(out, \(x) {
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

paws_error_code <- function(error){
  return(error[["error_response"]][["__type"]] %||% error[["error_response"]][["Code"]])
}

split_vec <- function(vec, len, max_len = length(vec)){
  start <- seq(1, max_len, len)
  end <- c(start[-1]-1, max_len)
  lapply(seq_along(start), function(i) vec[start[i]:end[i]])
}

is_dir <- function(path){
  return(file.info(path)$isdir)
}

write_bin <- function(obj,
                      filename) {
  # If R version is 4.0.0 + then use writeBin due to long vector support
  # https://github.com/HenrikBengtsson/Wishlist-for-R/issues/97
  if (getRversion() > R_system_version("4.0.0")){
    writeBin(obj, filename)
  } else {
    # use readr if R version < 4.0.0 for extra speed
    if((!requireNamespace("brio", quietly = TRUE))){
      brio::write_file_raw(obj, filename)
    } else {
      base_write_loop(obj, filename)
    }
  }
  return(invisible(TRUE))
}

base_write_loop <- function(obj,
                            filename,
                            chunk_size = (GB*2)-2){
  # Only 2^31 - 1 bytes can be written in a single call
  max_len <- length(obj)
  start <- seq(1, max_len, chunk_size)
  end <- c(start[-1]-1, max_len)
  if (length(start) == 1) {
    writeBin(obj, filename)
  } else {
    # Open for reading and appending.
    con <- file(filename, "a+b")
    on.exit(close(con))
    sapply(seq_along(start), function(i){writeBin(obj[start[i]:end[i]], con)})
  }
}

camel_to_snake = function(name) {
  name = gsub('(.)([A-Z][a-z]+)', '\\1_\\2', name)
  return(tolower(gsub('([a-z0-9])([A-Z])', '\\1_\\2', name)))
}

list_zip = function(...){
  kwargs = list(...)
  .mapply(list, kwargs, NULL)
}
