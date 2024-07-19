#' @include zzz.R
#' @include utils.R

#' @import R6
#' @import paws.storage
#' @import future
#' @import future.apply
#' @import data.table
#' @importFrom utils modifyList
#' @importFrom fs path path_join is_dir dir_exists dir_info fs_bytes
#' @importFrom curl curl

KB = 1024
MB = KB ^ 2

retry_api_call = function(expr, retries){
  if(retries == 0){
    return(eval.parent(substitute(expr)))
  }

  for (i in seq_len(retries + 1)){
    tryCatch({
      return(eval.parent(substitute(expr)))
    }, http_500 = function(err) {
      # https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
      # HTTP Status Code: 500
      #     Error Code: InternalError
      #     Description: An internal error occurred. Try again.
      #
      # HTTP Status Code: 501
      #     Error Code: NotImplemented
      #     Description: A header that you provided implies functionality that is not implemented.
      #
      # HTTP Status Code: 503
      #     Error Code: ServiceUnavailable
      #     Description: Reduce your request rate.
      #     Error Code: SlowDown
      #     Description: Reduce your request rate.
      #     Error Code: Busy
      #     Description: The service is unavailable. Try again later.
      #
      # HTTP Status Code: 503
      #     Error Code: NotImplemented
      #     Description: Reduce your request rate.
      if(i == (retries + 1))
        stop(err)
      time = 2**i * 0.1
      LOGGER$error("Request failed. Retrying in %s seconds...", time)
      Sys.sleep(time)
    }, error = function(err) {
      stop(err)
    })
  }
}

#' @title Access AWS S3 as if it were a file system.
#' @description This creates a file system "like" API based off \code{fs}
#'              (e.g. dir_ls, file_copy, etc.) for AWS S3 storage.
#' @export
S3FileSystem = R6Class("S3FileSystem",
  public = list(

    #' @field s3_cache
    #' Cache AWS S3
    s3_cache = list(),

    #' @field s3_cache_bucket
    #' Cached s3 bucket
    s3_cache_bucket = "",

    #' @field s3_client
    #' paws s3 client
    s3_client = NULL,

    #' @field region_name
    #' AWS region when creating new connections
    region_name = NULL,

    #' @field profile_name
    #' The name of a profile to use
    profile_name = NULL,

    #' @field multipart_threshold
    #' Threshold to use multipart
    multipart_threshold = NULL,

    #' @field request_payer
    #' Threshold to use multipart
    request_payer = NULL,

    #' @field pid
    #' Get the process ID of the R Session
    pid = Sys.getpid(),

    #' @description Initialize S3FileSystem class
    #' @param aws_access_key_id (character): AWS access key ID
    #' @param aws_secret_access_key (character): AWS secret access key
    #' @param aws_session_token (character): AWS temporary session token
    #' @param region_name (character): Default region when creating new connections
    #' @param profile_name (character): The name of a profile to use. If not given,
    #'              then the default profile is used.
    #' @param endpoint (character): The complete URL to use for the constructed client.
    #' @param disable_ssl (logical): Whether or not to use SSL. By default, SSL is used.
    #' @param multipart_threshold (\link[fs]{fs_bytes}): Threshold to use multipart instead of standard
    #'              copy and upload methods.
    #' @param request_payer (logical): Confirms that the requester knows that they
    #'              will be charged for the request.
    #' @param anonymous (logical): Set up anonymous credentials when connecting to AWS S3.
    #' @param ... Other parameters within \code{paws} client.
    initialize = function(aws_access_key_id = NULL,
                          aws_secret_access_key = NULL,
                          aws_session_token = NULL,
                          region_name = NULL,
                          profile_name = NULL,
                          endpoint = NULL,
                          disable_ssl = FALSE,
                          multipart_threshold = fs_bytes("2GB"),
                          request_payer = FALSE,
                          anonymous = FALSE,
                          ...){
      stopifnot(
        "`aws_access_key_id` is required to be a character vector" = (
          is.character(aws_access_key_id) || is.null(aws_access_key_id)
        ),
        "`aws_secret_access_key` is required to be a character vector" = (
          is.character(aws_secret_access_key) || is.null(aws_secret_access_key)
        ),
        "`aws_session_token` is required to be a character vector" = (
          is.character(aws_session_token) || is.null(aws_session_token)
        ),
        "`region_name` is required to be a character vector" = (
          is.character(region_name) || is.null(region_name)
        ),
        "`profile_name` is required to be a character vector" = (
          is.character(profile_name) || is.null(profile_name)
        ),
        "`endpoint` is required to be a character vector" = (
          is.character(endpoint) || is.null(endpoint)
        ),
        "`disable_ssl` is required to be a character vector" = (
          is.logical(disable_ssl)
        ),
        "`multipart_threshold` is required to be a numeric vector" = (
          is.numeric(multipart_threshold)
        ),
        "`request_payer` is required to be a numeric vector" = (
          is.logical(request_payer)
        )
      )
      self$profile_name = profile_name
      self$region_name = region_name %||% get_region(profile_name)
      config = private$.cred_set(
        aws_access_key_id,
        aws_secret_access_key,
        aws_session_token,
        self$profile_name,
        self$region_name,
        endpoint,
        disable_ssl,
        anonymous,
        ...
      )
      self$multipart_threshold = multipart_threshold
      self$request_payer = if(request_payer) "requester" else NULL
      self$s3_client = paws.storage::s3(config)
    },

    ############################################################################
    # File methods
    ############################################################################

    #' @description Change file permissions
    #' @param path (character): A character vector of path or s3 uri.
    #' @param mode (character): A character of the mode
    #' @return character vector of s3 uri paths
    file_chmod = function(path,
                          mode = c(
                            'private',
                            'public-read',
                            'public-read-write',
                            'authenticated-read',
                            'aws-exec-read',
                            'bucket-owner-read',
                            'bucket-owner-full-control')){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      mode = match.arg(mode)
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      s3_parts = lapply(path, private$.s3_split_path)
      future_lapply(seq_along(s3_parts), function(i){
          retry_api_call(
            self$s3_client$put_object_acl(
              Bucket = s3_parts[[i]]$Bucket,
              Key = s3_parts[[i]]$Key,
              VersionId = s3_parts[[i]]$VersionId,
              ACL = mode
            ), self$retries)
        },
        future.seed = length(s3_parts)
      )
      return(private$.s3_build_uri(path))
    },

    #' @description copy files
    #' @param path (character): path to a local directory of file or a uri.
    #' @param new_path (character): path to a local directory of file or a uri.
    #' @param max_batch (\link[fs]{fs_bytes}): Maximum batch size being uploaded with each multipart.
    #' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
    #'              and the file exists an error will be thrown.
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_put_object}}
    #' @return character vector of s3 uri paths
    file_copy = function(path,
                         new_path,
                         max_batch = fs_bytes("100MB"),
                         overwrite = FALSE,
                         ...){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path),
        "`new_path` is required to be a character vector" = is.character(new_path),
        "`max_batch` is required to be class numeric" = is.numeric(max_batch),
        "`overwrite` is required to be class logical" = is.logical(overwrite),
        "`max_batch` is required to be greater than 5 MB" = (max_batch > 5*MB)
      )

      # s3 uri to s3 uri
      if (any(is_uri(path)) & any(is_uri(new_path))) {
        path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
        new_path = unname(vapply(new_path, private$.s3_strip_uri, FUN.VALUE = ""))

        file_size = self$file_info(path)$size

        multipart = self$multipart_threshold < file_size

        standard = list_zip(path[!multipart], new_path[!multipart])
        multipart = list_zip(path[multipart], new_path[multipart], file_size[multipart])

        kwargs = list(...)
        kwargs$overwrite = overwrite
        kwargs$RequestPayer = kwargs$RequestPayer %||% self$request_payer

        if (length(standard) > 0){
          future_lapply(standard, function(part){
              kwargs$src = part[[1]]
              kwargs$dest = part[[2]]
              do.call(private$.s3_copy_standard, kwargs)
            },
            future.seed = length(standard)
          )
        }
        if (length(multipart) > 0){
          kwargs$max_batch = max_batch
          lapply(multipart, function(part){
              kwargs$src = part[[1]]
              kwargs$dest = part[[2]]
              kwargs$size = part[[3]]
              do.call(private$.s3_copy_multipart, kwargs)
          })
        }
        self$clear_cache(private$.s3_pnt_dir(new_path))
        return(private$.s3_build_uri(new_path))
      # s3 uri to local
      } else if (any(is_uri(path)) & !any(is_uri(new_path))) {
        self$file_download(path, new_path, overwrite, ...)
      # local to s3 uri
      } else if (!any(is_uri(path)) & any(is_uri(new_path))) {
        self$file_upload(path, new_path, max_batch, overwrite, ...)
      }
    },

    #' @description Create file on AWS S3, if file already
    #'      exists it will be left unchanged.
    #' @param path (character): A character vector of path or s3 uri.
    #' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
    #'              and the file exists an error will be thrown.
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_put_object}}
    #' @return character vector of s3 uri paths
    file_create = function(path,
                           overwrite = FALSE,
                           ...){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path),
        "`overwrite` is required to be class logical" = is.logical(overwrite)
      )
      if(isFALSE(overwrite) & any(self$file_exists(path)))
        stop("File already exists and overwrite is set to `FALSE`", call. = FALSE)

      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      s3_parts = lapply(path, private$.s3_split_path)
      if (any(sapply(s3_parts, function(l) !is.null(l$VersionId))))
        stop("S3 does not support touching existing versions of files", call. = FALSE)

      kwargs = list(...)
      kwargs$RequestPayer = kwargs$RequestPayer %||% self$request_payer
      resp = lapply(seq_along(s3_parts), function(i){
        kwargs$Bucket = s3_parts[[i]]$Bucket
        kwargs$Key = s3_parts[[i]]$Key
        kwargs$VersionId = s3_parts[[i]]$VersionId
        resp = NULL
        resp = retry_api_call(
          tryCatch({
            do.call(self$s3_client$put_object, kwargs)
            } # TODO: handle aws error
            ), self$retries)
        self$clear_cache(private$.s3_pnt_dir(path[i]))
        return(resp)
      })
      return(private$.s3_build_uri(path))
    },

    #' @description Delete files in AWS S3
    #' @param path (character): A character vector of paths or s3 uris.
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_delete_objects}}
    #' @return character vector of s3 uri paths
    file_delete = function(path,
                           ...){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      origin_path = path = unname(vapply(path, private$.s3_strip_uri, dir = T, FUN.VALUE = ""))
      s3_parts = unname(lapply(path, private$.s3_split_path))

      # Check which bucket require versioning
      buckets = unique(vapply(s3_parts, function(x) x$Bucket, FUN.VALUE = ""))
      status = vapply(buckets, private$.s3_is_bucket_version, FUN.VALUE = logical(1))

      if (any(status)){
        path_version = grepl(paste0("^", buckets[status]), path)
        path_version[path_version] = !grepl("?versionId=", path[path_version], fixed = T)
      } else {
        path_version = status
      }

      # Add file version uri
      if(length(path[path_version]) > 0){
        new_paths  = self$file_version_info(path[path_version])$uri
        new_paths = unname(vapply(new_paths, private$.s3_strip_uri, FUN.VALUE = ""))
        path = c(path[!path_version], new_paths)
        s3_parts = unname(lapply(path, private$.s3_split_path))
      }

      # re-apply directory "/" so that they can be removed
      dirs = self$is_dir(path)
      if(length(path[dirs]) > 0) {
        s3_dir = lapply(s3_parts[dirs], function(x) {x$Key = paste0(x$Key, "/"); x})
        s3_parts[dirs] = s3_dir
      }
      s3_parts = split(s3_parts, sapply(s3_parts, function(x) x$Bucket), drop = T)
      key_parts = lapply(s3_parts, split_vec, 1000)

      kwargs = list(...)
      kwargs$RequestPayer = kwargs$RequestPayer %||% self$request_payer
      lapply(key_parts, function(parts) {
        future_lapply(parts, function(part) {
            bucket = unique(sapply(part, function(x) x$Bucket))
            kwargs$Bucket = bucket
            kwargs$Delete = list(Objects = lapply(part, function(x) {
              Filter(Negate(is.null), x[2:3])
            }))
            retry_api_call(
              tryCatch({
                do.call(self$s3_client$delete_objects, kwargs)
              }), self$retries
            )
          },
          future.seed = length(parts)
        )
      })
      self$clear_cache(private$.s3_pnt_dir(origin_path))
      return(private$.s3_build_uri(origin_path))
    },

    #' @description Downloads AWS S3 files to local
    #' @param path (character): A character vector of paths or uris
    #' @param new_path (character): A character vector of paths to the new locations.
    #' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
    #'              and the file exists an error will be thrown.
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_get_object}}
    #' @return character vector of s3 uri paths
    file_download = function(path,
                             new_path,
                             overwrite=FALSE,
                             ...){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      file_exists = file.exists(new_path)
      if(any(file_exists) & !overwrite){
        stop(sprintf(
          "File '%s' already exist, please set `overwrite=TRUE`", paste(path_rel(new_path), ".")),
          call. = FALSE
        )
      }
      # create new_path parent folder
      dir.create(unique(dirname(new_path)), showWarnings = F, recursive = T)
      new_path = path_abs(new_path)

      if (length(new_path) == 1 && all(fs::is_dir(new_path))) {
        new_path = rep(new_path, length(path))
        new_path = paste(trimws(new_path, "right", "/"), basename(path), sep = "/")
        dir.create(unique(dirname(new_path)), showWarnings = F, recursive = T)
      }

      kwargs = list(...)
      kwargs$RequestPayer = kwargs$RequestPayer %||% self$request_payer
      future_lapply(seq_along(path), function(i){
          kwargs$src = path[[i]]
          kwargs$dest = new_path[[i]]
          do.call(private$.s3_download_file, kwargs)
        },
        future.seed = length(path)
      )
      return(new_path)
    },

    #' @description Check if file exists in AWS S3
    #' @param path (character) s3 path to check
    #' @return logical vector if file exists
    file_exists = function(path){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))

      # check if directory is in cache
      found = vector(length = length(path))
      if (length(self$s3_cache) > 0) {
        uri = unique(rbindlist(self$s3_cache)[["uri"]])
        found = path %in% vapply(uri, private$.s3_strip_uri, FUN.VALUE = "")
      }

      if(!any(found)){
        s3_parts = lapply(path[!found], private$.s3_split_path)
        exist  = future_vapply(seq_along(s3_parts), function(i) {
            retry_api_call(
              tryCatch({
                self$s3_client$head_object(
                  Bucket = s3_parts[[i]]$Bucket,
                  Key = s3_parts[[i]]$Key,
                  VersionId = s3_parts[[i]]$VersionId
                )
                return(TRUE)
                }, http_404 = function(e){
                  return(FALSE)
              }), self$retries
            )
          },
          FUN.VALUE = logical(1),
          future.seed = length(s3_parts)
        )
        found[!found] = exist
      }
      return(found)
    },

    #' @description Returns file information within AWS S3 directory
    #' @param path (character): A character vector of paths or uris.
    #' @return A data.table with metadata for each file. Columns returned are as follows.
    #' \itemize{
    #' \item{bucket_name (character): AWS S3 bucket of file}
    #' \item{key (character): AWS S3 path key of file}
    #' \item{uri (character): S3 uri of file}
    #' \item{size (numeric): file size in bytes}
    #' \item{type (character): file type (file or directory)}
    #' \item{etag (character): An entity tag is an opague identifier}
    #' \item{last_modified (POSIXct): Created date of file.}
    #' \item{delete_marker (logical): Specifies retrieved a logical marker}
    #' \item{accept_ranges (character): Indicates that a range of bytes was specified.}
    #' \item{expiration (character): File expiration}
    #' \item{restore (character): If file is archived}
    #' \item{archive_status (character): Archive status}
    #' \item{missing_meta (integer): Number of metadata entries not returned in "x-amz-meta" headers}
    #' \item{version_id (character): version id of file}
    #' \item{cache_control (character): caching behaviour for the request/reply chain}
    #' \item{content_disposition (character): presentational information of file}
    #' \item{content_encoding (character): file content encodings}
    #' \item{content_language (character): what language the content is in}
    #' \item{content_type (character): file MIME type}
    #' \item{expires (POSIXct): date and time the file is no longer cacheable}
    #' \item{website_redirect_location (character): redirects request for file to another}
    #' \item{server_side_encryption (character): File server side encryption}
    #' \item{metadata (list): metadata of file}
    #' \item{sse_customer_algorithm (character): server-side encryption with a customer-provided encryption key}
    #' \item{sse_customer_key_md5 (character): server-side encryption with a customer-provided encryption key}
    #' \item{ssekms_key_id (character): ID of the Amazon Web Services Key Management Service}
    #' \item{bucket_key_enabled (logical): s3 bucket key for server-side encryption with}
    #' \item{storage_class (character): file storage class information}
    #' \item{request_charged (character): indicates successfully charged for request}
    #' \item{replication_status (character): return specific header if request
    #'      involves a bucket that is either a source or a destination in a replication rule
    #'      \url{https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object}}
    #' \item{parts_count (integer): number of count parts the file has}
    #' \item{object_lock_mode (character): the file lock mode}
    #' \item{object_lock_retain_until_date (POSIXct): date and time of when object_lock_mode expires}
    #' \item{object_lock_legal_hold_status (character): file legal holding}
    #' }
    file_info = function(path){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      s3_parts = lapply(path, private$.s3_split_path)

      resp = future_lapply(seq_along(s3_parts), function(i){
          resp = retry_api_call(
            tryCatch({
              self$s3_client$head_object(
                Bucket = s3_parts[[i]]$Bucket,
                Key = s3_parts[[i]]$Key,
                VersionId = s3_parts[[i]]$VersionId
              )
            }), self$retries)
          resp$bucket_name = s3_parts[[i]]$Bucket
          resp$key = s3_parts[[i]]$Key
          key = (
            if(!is.null(s3_parts$VersionId))
              paste0(s3_parts[[i]]$Key, "?versionId=", s3_parts$VersionId)
            else s3_parts[[i]]$Key
          )
          resp$uri = self$path(
            s3_parts[[i]]$Bucket,
            key
          )
          resp$size = resp$ContentLength
          resp$ContentLength = NULL
          resp$type = if (endsWith(s3_parts[[i]]$Key, "/")) "directory" else "file"
          resp[lengths(resp) == 0] = lapply(resp[lengths(resp) == 0], as.na)
          as.data.table(resp)
        },
        future.seed = length(s3_parts)
      )
      dt = rbindlist(resp)
      names(dt) = camel_to_snake(names(dt))
      setnames(dt, "e_tag", "etag")
      setcolorder(dt,
        c("bucket_name", "key", "uri", "size", "type", "etag", "last_modified")
      )
      dt$size = fs::fs_bytes(dt$size)
      return(dt)
    },

    #' @description Move files to another location on AWS S3
    #' @param path (character): A character vector of s3 uri
    #' @param new_path (character): A character vector of s3 uri.
    #' @param max_batch (\link[fs]{fs_bytes}): Maximum batch size being uploaded with each multipart.
    #' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
    #'              and the file exists an error will be thrown.
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_copy_object}}
    #' @return character vector of s3 uri paths
    file_move = function(path,
                         new_path,
                         max_batch = fs_bytes("100MB"),
                         overwrite = FALSE,
                         ...){
      path = private$.s3_build_uri(path)
      new_path = private$.s3_build_uri(new_path)
      self$file_copy(path, new_path, max_batch, overwrite, ...)
      self$file_delete(path)
      return(new_path)
    },

    #' @description Return file size in bytes
    #' @param path (character): A character vector of s3 uri
    file_size = function(path){
      return(self$file_info(path)$size)
    },

    #' @description Streams in AWS S3 file as a raw vector
    #' @param path (character): A character vector of paths or s3 uri
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_get_object}}
    #' @return list of raw vectors containing the contents of the file
    file_stream_in = function(path,
                              ...){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      kwargs = list(...)
      kwargs$RequestPayer = kwargs$RequestPayer %||% self$request_payer

      future_lapply(path, function(p){
          kwargs$src = p
          do.call(private$.s3_stream_in_file, kwargs)
        },
        future.seed = length(path)
      )
    },

    #' @description Streams out raw vector to AWS S3 file
    #' @param path (character): A character vector of paths or s3 uri
    #' @param obj (raw|character): A raw vector, rawConnection, url to be streamed up to AWS S3.
    #' @param max_batch (\link[fs]{fs_bytes}): Maximum batch size being uploaded with each multipart.
    #' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
    #'              and the file exists an error will be thrown.
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_put_object}}
    #' @return character vector of s3 uri paths
    file_stream_out = function(obj,
                               path,
                               max_batch = fs_bytes("100MB"),
                               overwrite = FALSE,
                               ...){
      stopifnot(
        "`obj` is required to be a raw vector or list of raw vectors" = (
          inherits(obj, c("raw", "list", "character"))
        ),
        "`path` is required to be a character vector" = is.character(path),
        "`overwrite` is required to be class numeric" = is.numeric(max_batch),
        "`overwrite` is required to be class logical" = is.logical(overwrite),
        "`max_batch` is required to be greater than 5*MB" = (max_batch > 5*MB)
      )
      if(is.list(obj)){
        stopifnot(
          "Each element of the list `obj` needs to be a raw object." = (
            all(sapply(obj, inherits, what = "raw"))
          )
        )
      }
      kwargs = list(...)
      kwargs$overwrite = overwrite
      kwargs$RequestPayer = kwargs$RequestPayer %||% self$request_payer

      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))

      obj_type = if (is.list(obj)) vapply(obj, class, "") else class(obj)
      if (length(unique(obj_type)) > 1)
        stop(sprintf(
            "`obj` needs to be of the same type: %s", paste(obj_type, collapse = ", ")
          ), call. = FALSE
        )

      obj = if(is.list(obj)) obj else list(obj)

      if (unique(obj_type) == "character"){
        url_stream = list_zip(obj, path, max_batch)
        future_lapply(url_stream, function(part) {
            kwargs$obj = part[[1]]
            kwargs$dest = part[[2]]
            do.call(private$.s3_stream_out_url, kwargs)
          },
          future.seed = length(url_stream)
        )
      } else{
        obj_size = lengths(obj)
        multipart = self$multipart_threshold < obj_size

        standard = list_zip(obj[!multipart], path[!multipart], obj_size[!multipart])
        multipart = list_zip(
          lapply(obj[multipart],split_vec, len = max_batch),
          path[multipart],
          obj_size[multipart]
        )

        if (length(standard) > 0){
          future_lapply(standard, function(part){
              kwargs$obj = part[[1]]
              kwargs$dest = part[[2]]
              kwargs$size = part[[3]]
              do.call(private$.s3_stream_out_file, kwargs)
            },
            future.seed = length(standard)
          )
        }
        if (length(multipart) > 0){
          kwargs$max_batch = max_batch
          lapply(seq_along(multipart), function(i) {
            kwargs$obj = multipart[[i]][[1]]
            kwargs$dest = multipart[[i]][[2]]
            kwargs$size = multipart[[i]][[3]]
            do.call(private$.s3_stream_out_multipart_file, kwargs)
          })
        }
      }
      self$clear_cache(path)
      return(private$.s3_build_uri(path))
    },

    #' @description return the name which can be used as a temporary file
    #' @param pattern (character): A character vector with the non-random portion of the name.
    #' @param tmp_dir (character): The directory the file will be created in.
    #' @param ext (character): A character vector of one or more paths.
    #' @return character vector of s3 uri paths
    file_temp = function(pattern = "file", tmp_dir = "", ext = ""){
      stopifnot(
        "`pattern` is required to be a character vector" = is.character(pattern),
        "`tmp_dir` is required to be a character vector" = is.character(tmp_dir),
        "`ext` is required to be a character vector" = is.character(ext)
      )
      if(nzchar(tmp_dir)) {
        tmp_dir = unname(vapply(tmp_dir, private$.s3_strip_uri, FUN.VALUE = ""))
        self$s3_cache_bucket = str_split(tmp_dir, "/", 2, fixed = T)[[1]][[1]]
      } else if (is.null(self$s3_cache_bucket)) {
        tmp_dir = self$s3_cache_bucket
      }
      has_extension = nzchar(ext)
      ext[has_extension] = paste0(".", sub("^[.]", "", ext[has_extension]))
      id = paste0(sample(c(letters, 1:9), replace = T, 12), collapse = "")
      file_id = paste0(pattern, id, ext)
      return(self$path(tmp_dir, file_id))
    },

    #' @description Delete file tags
    #' @param path (character): A character vector of paths or s3 uri
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_put_object}}
    #' @return character vector of s3 uri paths
    file_tag_delete = function(path){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      found = self$file_exists(path)
      if(!any(self$file_exists(path))){
        stop(sprintf(
          "Following files don't exist:\n'%s'", paste(private$.s3_build_uri(path[!found]), collapse = "',\n'")),
          call. = F
        )
      }
      s3_parts = lapply(path, private$.s3_split_path)
      future_lapply(seq_along(s3_parts), function(i){
          retry_api_call(
            self$s3_client$delete_object_tagging(
              Bucket = s3_parts[[i]]$Bucket,
              Key = s3_parts[[i]]$Key,
              VersionId = s3_parts[[i]]$VersionId
            ), self$retries
          )
        },
        future.seed = length(s3_parts)
      )
      return(private$.s3_build_uri(path))
    },

    #' @description Get file tags
    #' @param path (character): A character vector of paths or s3 uri
    #' @return data.table of file version metadata
    #' \itemize{
    #' \item{bucket_name (character): AWS S3 bucket of file}
    #' \item{key (character): AWS S3 path key of file}
    #' \item{uri (character): S3 uri of file}
    #' \item{size (numeric): file size in bytes}
    #' \item{version_id (character): version id of file}
    #' \item{tag_key (character): name of tag}
    #' \item{tag_value (character): tag value}
    #' }
    file_tag_info = function(path){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      found = self$file_exists(path)
      if(!any(self$file_exists(path))){
        stop(sprintf(
          "Following files don't exist:\n'%s'", paste(private$.s3_build_uri(path[!found]), collapse = "',\n'")),
          call. = F
        )
      }
      s3_parts = lapply(path, private$.s3_split_path)
      kwargs = list()
      out = future_lapply(seq_along(s3_parts), function(i){
          kwargs$Bucket = s3_parts[[i]]$Bucket
          kwargs$Key = s3_parts[[i]]$Key
          kwargs$VersionId = s3_parts[[i]]$VersionId
          resp = retry_api_call(
            do.call(self$s3_client$get_object_tagging, kwargs),
            self$retries
          )
          tag = suppressWarnings(rbindlist(resp$TagSet, fill = T))
          if(nrow(tag) == 0)
            tag = data.table("tag_key" = "", tag_key = "")
          names(tag) = c("tag_key", "tag_value")
          tag$version_id = resp$VersionId
          tag$bucket_name = s3_parts[[i]]$Bucket
          tag$key = s3_parts[[i]]$Key
          tag$uri = self$path(
            s3_parts[[i]]$Bucket,
            if(!is.null(s3_parts[[i]]$VersionId))
              paste0(s3_parts[[i]]$Key, "?versionId=", s3_parts[[i]]$VersionId)
            else s3_parts[[i]]$Key
          )
          return(tag)
        },
        future.seed = length(s3_parts)
      )
      out = rbindlist(out)
      names(out) = camel_to_snake(names(out))
      setcolorder(out, c("bucket_name", "key", "uri", "version_id"))
      return(out)
    },

    #' @description Update file tags
    #' @param path (character): A character vector of paths or s3 uri
    #' @param tags (list): Tags to be applied
    #' @param overwrite (logical): To overwrite tagging or to modify inplace. Default will
    #'             modify inplace.
    #' @return character vector of s3 uri paths
    file_tag_update = function(path,
                               tags,
                               overwrite=FALSE){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path),
        "`tags` is required to be a list" = is.list(tags),
        "`tags` is required to be a named list" = !is.null(names(tags)),
        "`overwrite` is required to be a logical vector" = is.logical(overwrite)
      )
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      found = self$file_exists(path)
      if(!any(self$file_exists(path))){
        stop(sprintf(
          "Following files don't exist:\n'%s'", paste(private$.s3_build_uri(path[!found]), collapse = "',\n'")),
          call. = F
        )
      }
      s3_parts = lapply(path, private$.s3_split_path)
      if (!overwrite) {
        kwargs = list()
        new_tags = future_lapply(seq_along(s3_parts), function(i){
            kwargs$Bucket = s3_parts[[i]]$Bucket
            kwargs$Key = s3_parts[[i]]$Key
            kwargs$VersionId = s3_parts[[i]]$VersionId
            resp = retry_api_call(
              do.call(self$s3_client$get_object_tagging, kwargs),
              self$retries
            )
            org_tag = lapply(resp$TagSet, function(x) x$Value)
            names(org_tag) = vapply(resp$TagSet, function(x) x$Key, FUN.VALUE = "")
            new_tags = modifyList(org_tag, tags)
            return(
              lapply(names(new_tags), function(n) list(Key=n, Value=new_tags[[n]]))
            )
          },
          future.seed = length(s3_parts)
        )
      } else if (overwrite) {
        new_tags = list(lapply(names(tags), function(n) list(Key=n, Value=tags[[n]])))
      }

      kwargs = list()
      future_lapply(seq_along(s3_parts), function(i){
          kwargs$Bucket = s3_parts[[i]]$Bucket
          kwargs$Key = s3_parts[[i]]$Key
          kwargs$VersionId = s3_parts[[i]]$VersionId
          kwargs$Tagging = list("TagSet" = new_tags[[i]])
          resp = retry_api_call(
            do.call(self$s3_client$put_object_tagging, kwargs),
            self$retries
          )
        },
        future.seed = length(s3_parts)
      )
      return(private$.s3_build_uri(path))
    },

    #' @description Similar to `fs::file_touch` this does not create the file if
    #'              it does not exist. Use `s3fs$file_create()` to do this if needed.
    #' @param path (character): A character vector of paths or s3 uri
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_copy_object}}
    #' @note This method will only update the modification time of the AWS S3 object.
    #' @return character vector of s3 uri paths
    file_touch = function(path,
                          ...) {
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))

      found = self$file_exists(path)

      metadata = private$.s3_metadata(path, ...)
      standard_path = list_zip(path, metadata)

      kwargs = list(...)
      kwargs$overwrite = TRUE
      kwargs$MetadataDirective = "REPLACE"
      kwargs$RequestPayer = kwargs$RequestPayer %||% self$request_payer

      s3fs_touch = list("s3fs_touch" = now_utc())
      if(length(standard_path) > 0){
        lapply(standard_path, function(s) {
          kwargs$src = s[[1]]
          kwargs$dest = s[[1]]
          kwargs$Metadata = modifyList(s[2], s3fs_touch)
          do.call(private$.s3_copy_standard, kwargs)
        })
      }
      return(private$.s3_build_uri(path))
    },

    #' @description Uploads files to AWS S3
    #' @param path (character): A character vector of local file paths to upload to AWS S3
    #' @param new_path (character): A character vector of AWS S3 paths or uri's of the new locations.
    #' @param max_batch (\link[fs]{fs_bytes}): Maximum batch size being uploaded with each multipart.
    #' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
    #'              and the file exists an error will be thrown.
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_put_object}}
    #'              and \code{\link[paws.storage]{s3_create_multipart_upload}}
    #' @return character vector of s3 uri paths
    file_upload = function(path,
                           new_path,
                           max_batch = fs_bytes("100MB"),
                           overwrite = FALSE,
                           ...){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path),
        "Please use `dir_upload` to upload directories to AWS S3" = !fs::is_dir(path),
        "`new_path` is required to be a character vector" = is.character(new_path),
        "`max_batch` is required to be class numeric" = is.numeric(max_batch),
        "`overwrite` is required to be class logical" = is.logical(overwrite),
        "`max_batch` is required to be greater than 5*MB" = (max_batch > 5*MB)
      )
      new_path = unname(vapply(new_path, private$.s3_strip_uri, FUN.VALUE = ""))
      path = path_abs(path)

      if (length(new_path) == 1 & !nzchar(self$path_ext(new_path))) {
        new_path = rep(trimws(new_path, "right", "/"), length(path))
        new_path = paste(new_path, basename(path), sep = "/")
      }
      if(length(path) != length(new_path))
        stop(
          "Number of source files doesn't match number ",
          "of file destination. Please check source and destination files.",
          call. = FALSE
        )

      src_exist = file.exists(path)
      if(any(!src_exist)){
        stop(sprintf(
            "File '%s' doesn't exist, please check `path` parameter.",
            paste(path[!src_exist], collapse = "', '")),
          call. = F
        )
      }
      path_size = file.size(path)
      multipart = self$multipart_threshold < path_size

      kwargs = list(...)
      kwargs$overwrite = overwrite
      kwargs$RequestPayer = kwargs$RequestPayer %||% self$request_payer

      # split source files
      standard = list_zip(path[!multipart], path_size[!multipart], new_path[!multipart])
      multipart = list_zip(path[multipart], path_size[multipart], new_path[multipart])
      if (length(standard) > 0){
        future_lapply(standard, function(part){
            kwargs$src = part[[1]]
            kwargs$dest = part[[3]]
            kwargs$size = part[[2]]
            do.call(private$.s3_upload_standard_file, kwargs)
          },
          future.seed = length(standard)
        )
      }
      if (length(multipart) > 0){
        future_lapply(multipart, function(part){
            kwargs$src = part[[1]]
            kwargs$dest = part[[3]]
            kwargs$size = part[[2]]
            kwargs$max_batch = max_batch
            do.call(private$.s3_upload_multipart_file, kwargs)
          },
          future.seed = length(multipart)
        )
      }
      self$clear_cache(private$.s3_pnt_dir(new_path))
      return(private$.s3_build_uri(new_path))
    },

    #' @description Generate presigned url for S3 object
    #' @param path (character): A character vector of paths or uris
    #' @param expiration (numeric): The number of seconds the presigned url is
    #'              valid for. By default it expires in an hour (3600 seconds)
    #' @param ... parameters passed to \code{\link[paws.storage]{s3_get_object}}
    #' @return return character of urls
    file_url = function(path,
                        expiration = 3600L,
                        ...){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path),
        "`expiration` is required to be a numeric vector" = is.numeric(expiration)
      )
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      s3_parts = lapply(path, private$.s3_split_path)
      args = list(...)

      kwargs = list(
        client_method = "get_object",
        expires_in = expiration,
        http_method = args$http_method
      )
      args$http_method = NULL
      params = list(RequestPayer = args$RequestPayer %||% self$request_payer)
      args$RequestPayer = NULL
      kwargs$params = modifyList(params, args)

      return(vapply(s3_parts, function(prt) {
          kwargs$params$Bucket = prt$Bucket
          kwargs$params$Key = prt$Key
          kwargs$params$VersionId = prt$VersionId
          do.call(self$s3_client$generate_presigned_url, kwargs)
        }, FUN.VALUE = "")
      )
    },

    #' @description Get file versions
    #' @param path (character): A character vector of paths or uris
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_list_object_versions}}
    #' @return return data.table with file version info, columns below:
    #' \itemize{
    #' \item{bucket_name (character): AWS S3 bucket of file}
    #' \item{key (character): AWS S3 path key of file}
    #' \item{uri (character): S3 uri of file}
    #' \item{size (numeric): file size in bytes}
    #' \item{version_id (character): version id of file}
    #' \item{owner (character): file owner}
    #' \item{etag (character): An entity tag is an opague identifier}
    #' \item{last_modified (POSIXct): Created date of file.}
    #' }
    file_version_info = function(path, ...){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      s3_parts = lapply(path, private$.s3_split_path)

      found = self$file_exists(path)
      if(!any(self$file_exists(path))){
        stop(sprintf(
          "Following files don't exist:\n'%s'", paste(private$.s3_build_uri(path[!found]), collapse = "',\n'")),
          call. = F
        )
      }
      kwargs = list(...)
      kwargs$RequestPayer = kwargs$RequestPayer %||% self$request_payer
      out = future_lapply(seq_along(s3_parts), function(i){
          j = 1
          out = list()
          while(!identical(kwargs$VersionIdMarker, character(0))){
            resp = retry_api_call(
              self$s3_client$list_object_versions(
                Bucket = s3_parts[[i]]$Bucket,
                Prefix = s3_parts[[i]]$Key,
                VersionIdMarker = kwargs$VersionIdMarker,
                KeyMarker = kwargs$KeyMarker
              ), self$retries
            )
            kwargs$VersionIdMarker = resp$VersionIdMarker
            kwargs$KeyMarker = resp$KeyMarker

            df = suppressWarnings(
              rbindlist(
                lapply(resp$Versions, function(v)
                  list(
                    size = v$Size,
                    version_id =v$VersionId,
                    owner = v$Owner$DisplayName,
                    etag = v$ETag,
                    last_modified = v$LastModified
                  )
                )
              )
            )
            df$bucket_name = s3_parts[[i]]$Bucket
            df$key = s3_parts[[i]]$Key
            df$uri = self$path(
              s3_parts[[i]]$Bucket,
              if(!is.null(df$version_id))
                paste0(s3_parts[[i]]$Key, "?versionId=", df$version_id)
              else s3_parts[[i]]$Key
            )
            out[[j]] = df
            j = j + 1
          }
          dt = rbindlist(out)
          setcolorder(dt, c("bucket_name", "key", "uri"))
          return(dt)
        },
        future.seed = length(s3_parts)
      )
      return(rbindlist(out))
    },

    ############################################################################
    # Test File methods
    ############################################################################

    #' @description Test for file types
    #' @param path (character): A character vector of paths or uris
    #' @return logical vector if object is a file
    is_file = function(path){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      return(self$file_exists(path))
    },

    #' @description Test for file types
    #' @param path (character): A character vector of paths or uris
    #' @return logical vector if object is a directory
    is_dir = function(path){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      path = unname(vapply(path,  private$.s3_strip_uri, FUN.VALUE = ""))
      # check if directory is in cache
      found = path %in% names(self$s3_cache)
      if (length(self$s3_cache) > 0) {
        uri = unique(rbindlist(self$s3_cache)[get("type") == "directory"][["uri"]])
        found[!found] = path[!found] %in% vapply(uri, private$.s3_strip_uri, FUN.VALUE = "")
      }
      found[!found] = self$dir_exists(path[!found])
      return(found)
    },

    #' @description Test for file types
    #' @param path (character): A character vector of paths or uris
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_list_objects_v2}}
    #' @return logical vector if object is a `AWS S3` bucket
    is_bucket = function(path, ...){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      found = path %in% private$.s3_bucket_ls()$bucket_name

      # bucket not owned by user
      s3_parts = lapply(path[!found], private$.s3_split_path)
      bucket_names = vapply(
        s3_parts, function(x) x$Bucket, FUN.VALUE = character(1)
      )
      mod_found = path[!found] %in% bucket_names
      exist = future_vapply(seq_along(s3_parts[mod_found]), function(i){
          tryCatch({
            self$s3_client$list_objects_v2(
              Bucket = s3_parts[mod_found][[i]]$Bucket,
              MaxKeys = 1,
              ...
            )
            return(TRUE)
          }, error = function(e){
            return(FALSE)
          })
        },
        FUN.VALUE = logical(1),
        future.seed = length(s3_parts[mod_found])
      )
      found[!found][mod_found] = exist
      return(found)
    },

    #' @description Test for file types
    #' @param path (character): A character vector of paths or uris
    #' @return logical vector if file is empty
    is_file_empty = function(path){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      return(self$file_info(path)$size == 0)
    },

    ############################################################################
    # Bucket methods
    ############################################################################

    #' @description Change bucket permissions
    #' @param path (character): A character vector of path or s3 uri.
    #' @param mode (character): A character of the mode
    #' @return character vector of s3 uri paths
    bucket_chmod = function(path,
                            mode = c(
                              "private",
                              "public-read",
                              "public-read-write",
                              "authenticated-read")){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      mode = match.arg(mode)
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      s3_parts = lapply(path, private$.s3_split_path)
      future_lapply(seq_along(s3_parts), function(i){
          retry_api_call(
            self$s3_client$put_bucket_acl(
              Bucket = s3_parts[[i]]$Bucket,
              ACL = mode
            ), self$retries)
        },
        future.seed = length(s3_parts)
      )
      return(private$.s3_build_uri(path))
    },

    #' @description Create bucket
    #' @param path (character): A character vector of path or s3 uri.
    #' @param region_name (character): aws region
    #' @param mode (character): A character of the mode
    #' @param versioning (logical): Whether to set the bucket to versioning or not.
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_create_bucket}}
    #' @return character vector of s3 uri paths
    bucket_create = function(path,
                             region_name = NULL,
                             mode = c(
                               "private",
                               "public-read",
                               "public-read-write",
                               "authenticated-read"),
                             versioning = FALSE,
                             ...){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path),
        "`region_name` is required to be a character vector" = (
          is.character(region_name) || is.null(region_name)
        ),
        "`versioning` is required to be a logical vector" = is.logical(versioning)
      )
      mode = match.arg(mode)
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))

      s3_parts = lapply(path, private$.s3_split_path)
      kwargs = list(...)
      kwargs$RequestPayer = kwargs$RequestPayer %||% self$request_payer
      kwargs$CreateBucketConfiguration = list(
        'LocationConstraint' = region_name %||% self$region_name
      )
      kwargs$ACL = mode

      future_lapply(seq_along(s3_parts), function(i){
          kwargs$Bucket = s3_parts[[i]]$Bucket
          retry_api_call(
            do.call(self$s3_client$create_bucket, kwargs),
            self$retries
          )
          if (versioning)
            retry_api_call(
              self$s3_client$put_bucket_versioning(
                Bucket = kwargs$Bucket,
                VersioningConfiguration = list(
                  Status = "Enabled"
                )
              ),
              self$retries
            )
        },
        future.seed = length(s3_parts)
      )
      return(private$.s3_build_uri(path))
    },

    #' @description Delete bucket
    #' @param path (character): A character vector of path or s3 uri.
    bucket_delete = function(path){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      original_path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))

      # Delete contents from s3 bucket
      path = self$dir_ls(original_path, recurse = TRUE, refresh = TRUE)
      if(!identical(path, character(0)))
        self$file_delete(path)
      s3_parts = lapply(original_path, private$.s3_split_path)
      future_lapply(seq_along(s3_parts), function(i){
          retry_api_call(
            self$s3_client$delete_bucket(
              Bucket = s3_parts[[i]]$Bucket
            ), self$retries
          )
        },
        future.seed = length(s3_parts)
      )
      self$clear_cache("__buckets")
      return(private$.s3_build_uri(original_path))
    },

    ############################################################################
    # Directory methods
    ############################################################################

    #' @description Copies the directory recursively to the new location.
    #' @param path (character): path to a local directory of file or a uri.
    #' @param new_path (character): path to a local directory of file or a uri.
    #' @param max_batch (\link[fs]{fs_bytes}): Maximum batch size being uploaded with each multipart.
    #' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
    #'              and the file exists an error will be thrown.
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_put_object}}
    #'              and \code{\link[paws.storage]{s3_create_multipart_upload}}
    #' @return character vector of s3 uri paths
    dir_copy = function(path,
                        new_path,
                        max_batch = fs_bytes("100MB"),
                        overwrite = FALSE,
                        ...){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path),
        "`new_path` is required to be a character vector" = is.character(new_path),
        "`max_batch` is required to be class numeric" = is.numeric(max_batch),
        "`overwrite` is required to be class logical" = is.logical(overwrite),
        "`max_batch` is required to be greater than 5*MB" = (max_batch > 5*MB)
      )
      # s3 uri to s3 uri
      if (any(is_uri(path)) & any(is_uri(new_path))) {
        path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
        new_path = unname(vapply(new_path, private$.s3_strip_uri, FUN.VALUE = ""))

        # list all source files needing to be copied over
        src_files = lapply(path, self$dir_info, type = "file", recurse = T)

        # create destination file location
        dest_files = lapply(seq_along(src_files), function(x) fs::path(
          new_path[[x]], path_rel(src_files[[x]]$uri, self$path(path[x])))
        )

        file_size = lapply(src_files, function(x) x$size)
        multipart = lapply(file_size, function(x) self$multipart_threshold < x)

        src_files = lapply(path, self$dir_info, type = "file", recurse = T)
        fs::path(src_files[[1]]$bucket_name, src_files[[1]]$key)
        # split source files
        standard_path = list_zip(
          lapply(seq_along(src_files), function(i) fs::path(
            src_files[[i]]$bucket_name[!multipart[[i]]], src_files[[i]]$key[!multipart[[i]]])
          ),
          lapply(seq_along(dest_files), function(i) dest_files[[i]][!multipart[[i]]])
        )
        multipart_path = list_zip(
          lapply(seq_along(src_files), function(i) fs::path(
            src_files[[i]]$bucket_name[!multipart[[i]]], src_files[[i]]$key[!multipart[[i]]])
          ),
          lapply(seq_along(dest_files), function(i) dest_files[[i]][multipart[[i]]]),
          lapply(seq_along(file_size), function(i) file_size[[i]][multipart[[i]]])
        )

        kwargs = list(...)
        kwargs$overwrite = overwrite
        kwargs$RequestPayer = kwargs$RequestPayer %||% self$request_payer

        if (any(!vapply(multipart, any, FUN.VALUE = logical(1)))){
          lapply(standard_path, function(part){
            future_lapply(seq_along(part[[1]]), function(i) {
                kwargs$src = part[[1]][i]
                kwargs$dest = part[[2]][i]
                do.call(private$.s3_copy_standard, kwargs)
              },
              future.seed = length(part[[1]])
            )
          })
        }
        if (any(vapply(multipart, any, FUN.VALUE = logical(1)))){
          kwargs$max_batch = max_batch
          lapply(multipart_path, function(part){
            lapply(seq_along(part[[1]]), function(i) {
              kwargs$src = part[[1]][i]
              kwargs$dest = part[[2]][i]
              kwargs$size = part[[3]][i]
              do.call(private$.s3_copy_multipart, kwargs)
            })
          })
        }
        self$clear_cache(new_path)
        return(private$.s3_build_uri(new_path))
        # s3 uri to local
      } else if (any(is_uri(path)) & !any(is_uri(new_path))) {
        self$dir_download(path, new_path, overwrite, ...)
        # local to s3 uri
      } else if (!any(is_uri(path)) & any(is_uri(new_path))) {
        self$dir_upload(path, new_path, max_batch, overwrite, ...)
      }
    },

    #' @description Create empty directory
    #' @param path (character): A vector of directory or uri to be created in AWS S3
    #' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
    #'              and the file exists an error will be thrown.
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_put_object}}
    #' @return character vector of s3 uri paths
    dir_create = function(path,
                          overwrite=FALSE,
                          ...){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path),
        "`overwrite` is required to be class logical" = is.logical(overwrite)
      )
      if(!any(self$dir_exists(path))){
        path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
        s3_parts = lapply(path, private$.s3_split_path)
        if (any(sapply(s3_parts, function(l) !is.null(l$VersionId))))
          stop("S3 does not support touching existing versions of files", call. = F)
        resp = lapply(seq_along(s3_parts), function(i){
          resp = NULL
          resp = retry_api_call(
            tryCatch({
              self$s3_client$put_object(
                Bucket = s3_parts[[i]]$Bucket,
                Key = paste0(trimws(s3_parts[[i]]$Key, "right", "/"), "/"),
                ...
              )
            } # TODO: handle aws error
            ), self$retries
          )
          self$clear_cache(private$.s3_pnt_dir(path[[i]]))
          return(resp)
        })
        return(private$.s3_build_uri(path))
      }
      LOGGER$info("Directory already exists in AWS S3")
      return(path)
    },

    #' @description Delete contents and directory in AWS S3
    #' @param path (character): A vector of paths or uris to directories to be deleted.
    #' @return character vector of s3 uri paths
    dir_delete = function(path){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      path_uri = self$dir_ls(path, recurse = T)
      self$file_delete(Filter(nchar, c(path, path_uri)))
      # ensure nested directories are removed from cache
      self$clear_cache(path)
      return(private$.s3_build_uri(path))
    },

    #' @description Check if path exists in AWS S3
    #' @param path (character) aws s3 path to be checked
    #' @return character vector of s3 uri paths
    dir_exists = function(path = "."){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      if(any(path %in% c(".", "", "/")))
        return(TRUE)
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))

      # check cache first
      path = unname(vapply(path,  private$.s3_strip_uri, FUN.VALUE = ""))
      # check if directory is in cache
      found = path %in% names(self$s3_cache)
      if (length(self$s3_cache) > 0) {
        uri = trimws(
          unique(rbindlist(self$s3_cache)[get("type") == "directory"][["uri"]]),
          "right",
          "/"
        )
        found[!found] = path[!found] %in% vapply(uri, private$.s3_strip_uri, FUN.VALUE = "")
      }
      # look for directory in
      if (!any(found)) {
        s3_parts = lapply(path[!found], private$.s3_split_path)
        kwargs = list(MaxKeys = 1)
        kwargs$RequestPayer = kwargs$RequestPayer %||% self$request_payer
        exist_l = rep(TRUE, length(s3_parts))
        for(i in seq_along(s3_parts)){
          kwargs$Bucket = s3_parts[[i]]$Bucket
          kwargs$Prefix = paste0(trimws(s3_parts[[i]]$Key, "both", "/"), "/")
          resp = list()
          resp = retry_api_call(
            tryCatch({
              do.call(self$s3_client$list_objects_v2, kwargs)$Contents
            }, http_404 = function(e){
              NULL
            }), self$retries
          )
          exist_l[i] = length(resp) > 0
        }
        found[!found] = exist_l
      }
      return(found)
    },

    #' @description Downloads AWS S3 files to local
    #' @param path (character): A character vector of paths or uris
    #' @param new_path (character): A character vector of paths to the new locations.
    #'              Please ensure directories end with a \code{/}.
    #' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
    #'              and the file exists an error will be thrown.
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_get_object}}
    #' @return character vector of s3 uri paths
    dir_download = function(path,
                            new_path,
                            overwrite = FALSE,
                            ...){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path),
        "`new_path` is required to be a character vector" = is.character(new_path),
        "`overwrite` is required to be class logical" = is.logical(overwrite),
        "length of path must equal length to new_path" = (
          length(path) == length(new_path)
        )
      )
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      dir_exists = fs::dir_exists(new_path)
      if(all(dir_exists) & !overwrite){
        stop(sprintf(
            "Directory '%s' already exist, please set `overwrite=TRUE`",
            paste(path_rel(new_path[[dir_exists]]), "', '")
          ), call. = FALSE
        )
      }
      # create new_path parent folder
      lapply(new_path, dir.create, showWarnings = F, recursive = T)
      new_path = path_abs(new_path)

      # list all source files needing to be copied over
      src_files = lapply(path, self$dir_ls, type = "file", recurse = T)
      src_files = lapply(src_files, function(x) {
        unname(vapply(x, private$.s3_strip_uri, FUN.VALUE = ""))
      })
      # get all sub directories from parent path directory
      src_sub_dir = lapply(list_zip(src_files, path), function(sp){
          Filter(
            nzchar,
            gsub("\\.", "", unique(trimws(dirname(path_rel(unlist(sp[[1]]), sp[[2]])), "left", "/")))
          )
      })

      # create sub directories within new_path
      lapply(list_zip(src_sub_dir, new_path), function(Dir) {
        lapply(paste(trimws(Dir[[2]], "right", "/"), Dir[[1]], sep = "/"), function(folder){
          dir.create(folder, showWarnings = F, recursive = T)
        })
      })

      # create destination files
      dest_files = lapply(seq_along(src_files), function(x) fs::path(
        new_path[[x]], path_rel(src_files[[x]], path[x]))
      )

      paths = list_zip(src_files, dest_files)

      kwargs = list(...)
      kwargs$RequestPayer = kwargs$RequestPayer %||% self$request_payer

      lapply(paths, function(parts){
        future_lapply(seq_along(parts[[1]]), function(i){
              kwargs$src = parts[[1]][i]
              kwargs$dest = parts[[2]][i]
              do.call(private$.s3_download_file, kwargs)
          },
          future.seed = length(parts[[1]])
        )
      })
      return(path_rel(new_path))
    },

    #' @description Returns file information within AWS S3 directory
    #' @param path (character):A character vector of one or more paths. Can be path
    #'              or s3 uri.
    #' @param type (character): File type(s) to return. Default ("any") returns all
    #'              AWS S3 object types.
    #' @param glob (character): A wildcard pattern (e.g. \code{*.csv}), passed onto
    #'              \code{grep()} to filter paths.
    #' @param regexp (character): A regular expression (e.g. \code{[.]csv$}),
    #'              passed onto \code{grep()} to filter paths.
    #' @param invert (logical): If \code{code} return files which do not match.
    #' @param recurse (logical): Returns all AWS S3 objects in lower sub directories
    #' @param refresh (logical): Refresh cached in \code{s3_cache}.
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_list_objects_v2}}
    #' @return data.table with directory metadata
    #' \itemize{
    #' \item{bucket_name (character): AWS S3 bucket of file}
    #' \item{key (character): AWS S3 path key of file}
    #' \item{uri (character): S3 uri of file}
    #' \item{size (numeric): file size in bytes}
    #' \item{version_id (character): version id of file}
    #' \item{etag (character): An entity tag is an opague identifier}
    #' \item{last_modified (POSIXct): Created date of file}
    #' }
    dir_info = function(path = ".",
                        type = c("any", "bucket", "directory", "file"),
                        glob = NULL,
                        regexp = NULL,
                        invert = FALSE,
                        recurse = FALSE,
                        refresh = FALSE,
                        ...){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path),
        "`glob` is required to be a character vector" = (
          is.character(glob) || is.null(glob)
        ),
        "`regexp` is required to be a character vector" = (
          is.character(regexp) || is.null(regexp)
        ),
        "`invert` is required to be a character vector" = is.logical(invert),
        "`recurse` is required to be a logical vector" = is.logical(recurse),
        "`refresh` is required to be a logical vector" = is.logical(refresh)
      )
      Type = match.arg(type)
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      kwargs = list(...)
      kwargs$RequestPayer = kwargs$RequestPayer %||% self$request_payer
      if(all(private$.cache_s3_data(path)))
        private$.cache_s3_bucket(path)
      file = rbindlist(self$s3_cache[path])
      if (all(path %in% c("", "/", "."))){
        files = private$.s3_bucket_ls(refresh)
      } else {
        private$.cache_s3_bucket(path)
        kwargs$refresh = refresh
        kwargs$recurse = recurse
        files = rbindlist(lapply(
          path, function(p) {
            kwargs$path = p
            do.call(private$.s3_dir_ls, kwargs)
          }
        ))
      }
      if(nrow(files) == 0){
        return(suppressWarnings(data.table::data.table(
          "bucket_name"= character(),
          "key" = character(),
          "uri" = character(),
          "size" = numeric(),
          "type" = character(),
          "owner" = character(),
          "etag" = character(),
          "last_modified" = .POSIXct(character())
        )))
      }
      Key = Filter(Negate(is.na),
        vapply(str_split(path, "/", 2, fixed = T),
          function(p) p[2], FUN.VALUE = ""
        )
      )
      if (!identical(Key, character(0))) {
        files = files[
          # ensure only target directory items are returned
          grepl(paste0("^", Key, collapse = "|"), get("key"), perl = T) &
          # don't return directory path
          !(trimws(get("key"), "right", "/") %in% Key),
        ]
      }

      if(Type != "any") {
        files = files[get("type") %in% Type,]
      }
      if(!is.null(glob))
        files = files[grep(glob, get("key"), invert = invert),]
      if(!is.null(regexp))
        files = files[grep(regexp, get("key"), invert = invert),]

      files$size = fs::fs_bytes(files$size)
      return(files)
    },

    #' @description Returns file name within AWS S3 directory
    #' @param path (character):A character vector of one or more paths. Can be path
    #'              or s3 uri.
    #' @param type (character): File type(s) to return. Default ("any") returns all
    #'              AWS S3 object types.
    #' @param glob (character): A wildcard pattern (e.g. \code{*.csv}), passed onto
    #'              \code{grep()} to filter paths.
    #' @param regexp (character): A regular expression (e.g. \code{[.]csv$}),
    #'              passed onto \code{grep()} to filter paths.
    #' @param invert (logical): If \code{code} return files which do not match.
    #' @param recurse (logical): Returns all AWS S3 objects in lower sub directories
    #' @param refresh (logical): Refresh cached in \code{s3_cache}.
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_list_objects_v2}}
    #' @return character vector of s3 uri paths
    dir_ls = function(path = ".",
                      type = c("any", "bucket", "directory", "file"),
                      glob = NULL,
                      regexp = NULL,
                      invert = FALSE,
                      recurse = FALSE,
                      refresh = FALSE,
                      ...){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path),
        "`glob` is required to be a character vector" = (
          is.character(glob) || is.null(glob)
        ),
        "`regexp` is required to be a character vector" = (
          is.character(regexp) || is.null(regexp)
        ),
        "`invert` is required to be a character vector" = is.logical(invert),
        "`recurse` is required to be a logical vector" = is.logical(recurse),
        "`refresh` is required to be a logical vector" = is.logical(refresh)
      )
      Type = match.arg(type)
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      kwargs = list(...)
      kwargs$RequestPayer = kwargs$RequestPayer %||% self$request_payer
      if(recurse) {
        private$.cache_s3_bucket(path)
        kwargs$refresh = refresh
        kwargs$recurse = recurse
        files = rbindlist(lapply(
          path, function(p) {
            kwargs$path = p
            do.call(private$.s3_dir_ls, kwargs)
          }
        ))
      } else if (all(path %in% c("", "/", "."))){
        files = private$.s3_bucket_ls(refresh)
      } else {
        private$.cache_s3_bucket(path)
        kwargs$refresh = refresh
        files = rbindlist(lapply(
          path, function(p) {
            kwargs$path = p
            do.call(private$.s3_dir_ls, kwargs)
          }
        ))
        if(nrow(files) == 0 && all(sapply(path, function(p) grepl("/", p)))){
          files = rbindlist(lapply(private$.s3_pnt_dir(path), function(p) {
            kwargs$path = p
            do.call(private$.s3_dir_ls, kwargs)
          }))
        }
      }

      if(nrow(files) == 0)
        return(character(0))

      Key = Filter(Negate(is.na),
        vapply(str_split(path, "/", 2, fixed = T),
          function(p) p[2], FUN.VALUE = ""
        )
      )
      if (!identical(Key, character(0))) {
        files = files[
          # ensure only target directory items are returned
          grepl(paste0("^", Key, collapse = "|"), get("key"), perl = T) &
          # don't return directory path
          !(trimws(get("key"), "right", "/") %in% Key),
        ]
      }

      if(Type != "any")
        files = files[get("type") %in% Type,]
      if(!is.null(glob))
        files = files[grep(glob, get("key"), invert = invert),]
      if(!is.null(regexp))
        files = files[grep(regexp, get("key"), invert = invert),]

      return(if(identical(files$uri, NULL)) character(0) else files$uri)
    },

    #' @description Generate presigned url to list S3 directories
    #' @param path (character): A character vector of paths or uris
    #' @param expiration (numeric): The number of seconds the presigned url is
    #'              valid for. By default it expires in an hour (3600 seconds)
    #' @param recurse (logical): Returns all AWS S3 objects in lower sub directories
    #' @param ... parameters passed to \code{\link[paws.storage]{s3_list_objects_v2}}
    #' @return return character of urls
    dir_ls_url = function(path,
                          expiration = 3600L,
                          recurse = FALSE,
                          ...){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path),
        "`expiration` is required to be a numeric vector" = is.numeric(expiration),
        "`recurse` is required to be a character vector" = is.logical(recurse)
      )
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      s3_parts = lapply(path, private$.s3_split_path)
      args = list(...)

      kwargs = list(
        client_method = "list_objects_v2",
        expires_in = expiration,
        http_method = args$http_method
      )
      args$http_method = NULL
      params = list(RequestPayer = args$RequestPayer %||% self$request_payer)
      args$RequestPayer = NULL
      kwargs$params = modifyList(params, args)
      if(recurse){
        kwargs$params$Delimiter = NULL
      } else {
        kwargs$params$Delimiter = "/"
      }
      return(vapply(s3_parts, function(prt) {
          if (!nzchar(prt$Key))
            prefix = prt$Key
          if (nzchar(prt$Key))
            prefix = paste0(trimws(prt$Key, "left", "/"), "/")
          kwargs$params$Bucket = prt$Bucket
          kwargs$params$Prefix = prefix
          do.call(self$s3_client$generate_presigned_url, kwargs)
        }, FUN.VALUE = "")
      )
    },

    # Modified from fs:
    # https://github.com/r-lib/fs/blob/main/R/tree.R#L8-L36

    #' @description Print contents of directories in a tree-like format
    #' @param path (character): path A path to print the tree from
    #' @param recurse (logical): Returns all AWS S3 objects in lower sub directories
    #' @param ... Additional arguments passed to [s3_dir_ls].
    #' @return character vector of s3 uri paths
    dir_tree = function(path, recurse = TRUE, ...) {
      stopifnot(
        "`path` is required to be a character vector" = is.character(path),
        "`recurse` is required to be a logical vector" = is.logical(recurse)
      )
      files = self$dir_ls(path, recurse = recurse, ...)
      by_dir = split(files, self$path_dir(files))

      # import fs functions
      box_chars = pkg_method("box_chars", "fs")

      ch = box_chars()
      by_dir = private$.append_to_pnt_dir(by_dir)

      print_leaf = function(x, indent) {
        leafs = by_dir[[x]]
        for (i in seq_along(leafs)) {
          if (i == length(leafs)) {
            cat(indent, paste0(ch$l, ch$h, ch$h, " ", collapse = ""), self$path_file(leafs[[i]]), "\n", sep = "")
            print_leaf(leafs[[i]], paste0(indent, "    "))
          } else {
            cat(indent, paste0(ch$j, ch$h, ch$h, " ", collapse = ""), self$path_file(leafs[[i]]), "\n", sep = "")
            print_leaf(leafs[[i]], paste0(indent, paste0(ch$v, "   ", collapse = "")))
          }
        }
      }

      cat(path, "\n", sep = "")
      print_leaf(path, "")
      invisible(files)
    },

    #' @description Uploads local directory to AWS S3
    #' @param path (character): A character vector of local file paths to upload to AWS S3
    #' @param new_path (character): A character vector of AWS S3 paths or uri's of the new locations.
    #' @param max_batch (\link[fs]{fs_bytes}): Maximum batch size being uploaded with each multipart.
    #' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
    #'              and the file exists an error will be thrown.
    #' @param ... parameters to be passed to \code{\link[paws.storage]{s3_put_object}}
    #'              and \code{\link[paws.storage]{s3_create_multipart_upload}}
    #' @return character vector of s3 uri paths
    dir_upload = function(path,
                          new_path,
                          max_batch = fs_bytes("100MB"),
                          overwrite = FALSE,
                          ...){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path),
        "`new_path` is required to be a character vector" = is.character(new_path),
        "`max_batch` is required to be class numeric" = is.numeric(max_batch),
        "`overwrite` is required to be class logical" = is.logical(overwrite),
        "Please use `file_upload` to upload files to AWS S3" = all(fs::is_dir(path)),
        "length of path must equal length to new_path" = (length(path) == length(new_path)),
        "`max_batch` is required to be greater than 5*MB" = (max_batch > 5*MB)
      )
      new_path = unname(vapply(new_path, private$.s3_strip_uri, FUN.VALUE = ""))
      path = path_abs(path)

      # list all source files needing to be copied over
      src_files = lapply(path, fs::dir_info, type = "file", recurse = T)

      # create destination file location
      dest_files = lapply(seq_along(src_files), function(x) fs::path(
        new_path[[x]], path_rel(src_files[[x]]$path, path[x]))
      )

      file_size = lapply(src_files, function(x) x$size)
      multipart = lapply(file_size, function(x) self$multipart_threshold < x)

      # split source files
      standard_path = list_zip(
        lapply(seq_along(src_files), function(i) src_files[[i]]$path[!multipart[[i]]]),
        lapply(seq_along(dest_files), function(i) dest_files[[i]][!multipart[[i]]]),
        lapply(seq_along(file_size), function(i) file_size[[i]][!multipart[[i]]])
      )
      multipart_path = list_zip(
        lapply(seq_along(src_files), function(i) src_files[[i]]$path[!multipart[[i]]]),
        lapply(seq_along(dest_files), function(i) dest_files[[i]][multipart[[i]]]),
        lapply(seq_along(file_size), function(i) file_size[[i]][multipart[[i]]])
      )
      kwargs = list(...)
      kwargs$overwrite = overwrite
      kwargs$RequestPayer = kwargs$RequestPayer %||% self$request_payer
      if (any(!vapply(multipart, any, FUN.VALUE = logical(1)))) {
        lapply(standard_path, function(part){
          future_lapply(seq_along(part[[1]]), function(i) {
              kwargs$src = part[[1]][i]
              kwargs$dest = part[[2]][i]
              kwargs$size = part[[3]][i]
              do.call(private$.s3_upload_standard_file, kwargs)
            },
            future.seed = length(part[[1]])
          )
        })
      }
      if (any(vapply(multipart, any, FUN.VALUE = logical(1)))){
        kwargs$max_batch = max_batch
        lapply(multipart_path, function(part){
          lapply(seq_along(part[[1]]), function(i) {
            kwargs$src = part[[1]][i]
            kwargs$dest = part[[2]][i]
            kwargs$size = part[[3]][i]
            do.call(private$.s3_upload_multipart_file, kwargs)
          })
        })
      }
      self$clear_cache(self$path_dir(unlist(dest_files)))
      return(private$.s3_build_uri(path))
    },

    ############################################################################
    # Path methods
    ############################################################################

    #' @description Constructs a s3 uri path
    #' @param ... (character): Character vectors
    #' @param ext (character): An optional extension to append to the generated path
    #' @return character vector of s3 uri paths
    path = function(..., ext = ""){
      stopifnot(
        "ext is required to be a character" = is.character(ext)
      )
      path = trimws(fs::path(..., ext=ext), "left", "/")
      return(paste("s3:/", gsub("s3:/", "", path), sep = "/"))
    },

    #' @description Returns the directory portion of s3 uri
    #' @param path (character): A character vector of paths
    #' @return character vector of s3 uri paths
    path_dir = function(path){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      parts = str_split(path, "/", 2, fixed = T)
      root = (path == lapply(parts, function(p) p[[1]]))
      dir = dirname(path)
      dir[root] = path[root]
      return(private$.s3_build_uri(dir))
    },

    #' @description Returns the last extension for a path.
    #' @param path (character): A character vector of paths
    #' @return character s3 uri file extension
    path_ext = function(path){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      pattern = "(?<!^|[.]|/)[.]([^.]+)$"
      pos = regexpr(pattern, path, perl = TRUE)
      return(fifelse(pos > -1L, substring(path, pos + 1L), ""))
    },

    #' @description Removes the last extension and return the rest of the s3 uri.
    #' @param path (character): A character vector of paths
    #' @return character vector of s3 uri paths
    path_ext_remove = function(path){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      pattern = "(?<!^|[.]|/)[.]([^.]+)$"
      pos = regexpr(pattern, path, perl = TRUE)
      return(private$.s3_build_uri(Filter(nzchar, unlist(regmatches(path, pos, invert = TRUE)))))
    },

    #' @description Replace the extension with a new extension.
    #' @param path (character): A character vector of paths
    #' @param ext (character): New file extension
    #' @return character vector of s3 uri paths
    path_ext_set = function(path, ext){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      return(private$.s3_build_uri(paste(self$path_ext_remove(path), ext, sep = ".")))
    },

    #' @description Returns the file name portion of the s3 uri path
    #' @param path (character): A character vector of paths
    #' @return character vector of file names
    path_file = function(path){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      return(basename(path))
    },

    #' @description Construct an s3 uri path from path vector
    #' @param parts (character): A character vector of one or more paths
    #' @return character vector of s3 uri paths
    path_join = function(parts){
      stopifnot(
        "`parts` is required to be a character vector" = is.character(parts) || is.list(parts)
      )
      path = unname(vapply(parts, private$.s3_strip_uri, FUN.VALUE = ""))
      path = fs::path_join(path)
      return(vapply(path, function(x) {
          paste("s3:/", paste(trimws(gsub("s3://", "", x), whitespace="/"), collapse = "/"), sep = "/")
        }, FUN.VALUE = "")
      )
    },

    #' @description Split s3 uri path to core components bucket, key and version id
    #' @param path (character): A character vector of one or more paths or s3 uri
    #' @return list character vectors splitting the s3 uri path in "Bucket", "Key" and "VersionId"
    path_split = function(path){
      stopifnot(
        "`path` is required to be a character vector" = is.character(path)
      )
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      return(unname(lapply(path, private$.s3_split_path)))
    },

    ############################################################################
    # Helper methods
    ############################################################################

    #' @description Clear S3 Cache
    #' @param path (character): s3 path to be cl
    clear_cache = function(path = NULL){
      if(is.null(path))
        self$s3_cache = list()
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      for (p in path){
        self$s3_cache[[p]] = NULL
      }
    }
  ),
  active = list(

    #' @field retries
    #' number of retries
    retries = function(retries){
      if(missing(retries)) {
        return(private$.retries)
      } else {
        stopifnot(
           "retries requires to be numeric" = is.numeric(retries)
        )
        if(retries < 0)
          stop(
            "retries requires to be positive values", call. = F
          )
        private$.retries = retries
      }
    }
  ),
  private = list(
    .retries = 5,

    .append_to_pnt_dir = function(by_dir) {
      dirs = names(by_dir)
      pnt_dirs = self$path_dir(dirs)
      found = (dirs != pnt_dirs)
      dirs = dirs[found]
      pnt_dirs = pnt_dirs[found]
      dirs = split(dirs, pnt_dirs)
      # append any new directories
      for (pnt in names(dirs)){
        by_dir[[pnt]] = unique(c(by_dir[[pnt]], dirs[[pnt]]))
      }
      return(by_dir)
    },

    .cache_s3_data = function(path){
      return(names(self$s3_cache) %in% path)
      # !is.null(self$s3_cache[[path]])
    },

    .cache_s3_bucket = function(path){
      s3_parts = lapply(path, private$.s3_split_path)
      bucket_list = unique(sapply(s3_parts, function(x) x$Bucket))
      if(length(bucket_list) == 1)
        self$s3_cache_bucket = bucket_list
    },

    .s3_is_bucket_version = function(bucket){
      bucket_status = tryCatch({
        retry_api_call(
          self$s3_client$get_bucket_versioning(
            Bucket = bucket
          )$Status, self$retries
        )
      }, http_403 = function(err) {
        # if don't have permission assume bucket isn't versioned
        LOGGER$info("Assuming bucket '%s' isn't versioned.", bucket)
        return(character(0))
      })
      return(identical(bucket_status, "Enabled"))
    },

    .s3_bucket_ls = function(refresh=FALSE){
      if(!("__buckets" %in% names(self$s3_cache)) || refresh) {
        files = retry_api_call(
          tryCatch({
            self$s3_client$list_buckets()$Buckets
          } # TODO: handle aws error
          )
        , self$retries)
        self$s3_cache[["__buckets"]] = rbindlist(lapply(files, function(f){
          list(
            bucket_name = f$Name,
            key = "",
            uri = self$path(f$Name),
            size = 0,
            type = "bucket",
            owner = "",
            etag = "",
            last_modified = as.POSIXct(NA)
          )
        }))
      }
      return(self$s3_cache[["__buckets"]])
    },

    .s3_dir_ls = function(path,
                          max_keys=NULL,
                          delimiter="/",
                          prefix="",
                          recurse=FALSE,
                          refresh=FALSE,
                          ...){
      s3_parts = private$.s3_split_path(path)
      if (nzchar(s3_parts$Key)) {
        prefix = paste(trimws(s3_parts$Key, "left", "/"), prefix, sep = "/")
      }
      if(!(s3_parts$Key %in% names(self$s3_cache)) || refresh || is.null(delimiter)){
        LOGGER$debug("Get directory listing page for %s", path)
        kwargs = list(...)
        kwargs$Bucket = s3_parts$Bucket
        kwargs$Prefix = prefix
        kwargs$Delimiter = delimiter

        if (!is.null(max_keys)){
          kwargs$MaxKeys = max_keys
        }
        if(recurse){
          kwargs$Delimiter = NULL
        }
        i = 1
        resp = list()
        while (!identical(kwargs$ContinuationToken, character(0))){
          batch_resp = retry_api_call(
            tryCatch({
              do.call(self$s3_client$list_objects_v2, kwargs)
            }),
            self$retries
          )
          kwargs$ContinuationToken = batch_resp$NextContinuationToken
          resp[[i]] = batch_resp
          i = i + 1
        }

        s3_files = unlist(lapply(resp, function(i){
          lapply(i$Contents, function(c){
            list(
              bucket_name = s3_parts$Bucket,
              key = c$Key,
              uri = self$path(
                s3_parts$Bucket,
                c$Key,
                if(!is.null(s3_parts$VersionId))
                  paste0("?versionId=", s3_parts$VersionId)
                else ""
              ),
              size = c$Size,
              type = if (endsWith(c$Key, "/")) "directory" else "file",
              owner = (
                if (identical(c$Owner$DisplayName, character(0))) NA_character_ else c$Owner$DisplayName
              ),
              etag = c$ETag,
              last_modified = c$LastModified
            )
          })
        }), recursive = F)

        s3_dir = unlist(lapply(resp, function(i){
          i$CommonPrefixes
        }), recursive = F)
        s3_dir = lapply(s3_dir, function(l){
          list(
            bucket_name = s3_parts$Bucket,
            key = l$Prefix,
            uri = self$path(
              s3_parts$Bucket,
              l$Prefix,
              if(!is.null(s3_parts$VersionId))
                paste0("?versionId=", s3_parts$VersionId)
              else ""
            ),
            size = 0,
            type = "directory",
            owner = "",
            etag = "",
            last_modified = na_posixct()
          )
        })
        s3_ls = rbind(rbindlist(s3_files),rbindlist(s3_dir))
        if(!is.null(delimiter) && nrow(s3_ls) > 0)
          self$s3_cache[[path]] = s3_ls
      }
      return(self$s3_cache[[path]])
    },

    .s3_download_file = function(src, dest, ...) {
      s3_parts = private$.s3_split_path(src)
      retry_api_call(
        self$s3_client$download_file(
          Bucket = s3_parts$Bucket,
          Key = s3_parts$Key,
          VersionId = s3_parts$VersionId,
          Filename = dest,
          ...
        )$Body, self$retries)
      return(invisible(TRUE))
    },

    .s3_copy_standard = function(src,
                                 dest,
                                 overwrite = FALSE,
                                 ...){
      src_parts = private$.s3_split_path(src)
      dest_parts = private$.s3_split_path(dest)
      if (!is.null(dest_parts$VersionId))
        stop("Unable to copy to a versioned file", call. = FALSE)

      if(isFALSE(overwrite) & self$file_exists(dest))
        stop("File already exists and overwrite is set to `FALSE`", call. = FALSE)

      version_id = if(is.null(src_parts$VersionId)) "" else sprintf("?versionId=%s", src_parts$VersionId)
      copy_src = paste0(sprintf("/%s/%s",src_parts$Bucket, src_parts$Key), version_id)
      kwargs = list(...)
      kwargs$Bucket = dest_parts$Bucket
      kwargs$Key = dest_parts$Key
      kwargs$CopySource = copy_src
      retry_api_call(
        tryCatch({
          do.call(self$s3_client$copy_object, kwargs)
        }), self$retries)
      self$clear_cache(dest)
    },

    .s3_copy_multipart = function(src,
                                  dest,
                                  size,
                                  max_batch = 100 * MB,
                                  overwrite = FALSE,
                                  ...){
      src_parts = private$.s3_split_path(src)
      dest_parts = private$.s3_split_path(dest)

      if(isFALSE(overwrite) & self$file_exists(dest))
        stop("File already exists and overwrite is set to `FALSE`", call. = FALSE)

      if (!is.null(dest_parts$VersionId))
        stop("Unable to copy to a versioned file", call. = FALSE)

      upload_id = retry_api_call(
        self$s3_client$create_multipart_upload(
          Bucket = dest_parts$Bucket, Key = dest_parts$Key, ...
        )$UploadId,
        self$retries
      )
      version_id = if(is.null(src_parts$VersionId)) "" else sprintf("?versionId=%s", src_parts$VersionId)
      copy_src = paste0(sprintf("/%s/%s",src_parts$Bucket, src_parts$Key), version_id)
      start = as.numeric(seq(0, size-1, max_batch))
      end = c(start[-1]-1, size-1)
      chunks = sprintf("bytes=%s-%s", start, end)

      kwargs = list(...)
      kwargs$CopySource = copy_src
      kwargs$Bucket = dest_parts$Bucket
      kwargs$Key = dest_parts$Key
      kwargs$UploadId = upload_id

      tryCatch({
        parts = future_lapply(seq_along(chunks), function(i){
            kwargs$PartNumber = i
            kwargs$CopySourceRange = chunks[i]
            etag = retry_api_call(
              do.call(self$s3_client$upload_part_copy, kwargs)$CopyPartResult$ETag,
              self$retries
            )
            return(list(ETag = etag, PartNumber = i))
          },
          future.seed = length(chunks)
        )
        kwargs$MultipartUpload = list(Parts = parts)
        kwargs$CopySource = NULL
        retry_api_call(
          do.call(self$s3_client$complete_multipart_upload, kwargs),
          self$retries
        )
      }, error = function(cond){
        kwargs$MultipartUpload = NULL
        kwargs$CopySource = NULL
        retry_api_call(
          do.call(self$s3_client$abort_multipart_upload, kwargs),
          self$retries
        )
        LOGGER$error("Failed to copy file in multiparts")
        stop(cond)
      })
      self$clear_cache(dest)
    },

    .s3_metadata = function(path, ...){
      s3_parts = lapply(path, private$.s3_split_path)
      args = list(...)
      kwargs = list()
      kwargs$RequestPayer = args$RequestPayer %||% self$request_payer
      obj = future_lapply(seq_along(s3_parts), function(i){
          kwargs$Bucket = s3_parts[[i]]$Bucket
          kwargs$Key = s3_parts[[i]]$Key
          kwargs$VersionId = s3_parts[[i]]$VersionId
          retry_api_call(
            do.call(self$s3_client$head_object, kwargs)$Metadata,
            self$retries
          )
        },
        future.seed = length(s3_parts)
      )
    },

    .s3_stream_in_file = function(src, ...) {
      s3_parts = private$.s3_split_path(src)
      obj = retry_api_call(
        self$s3_client$get_object(
          Bucket = s3_parts$Bucket,
          Key = s3_parts$Key,
          VersionId = s3_parts$VersionId,
          ...
        )$Body, self$retries)
      return(obj)
    },

    .s3_stream_out_file = function(obj,
                                   dest,
                                   size,
                                   overwrite = FALSE,
                                   ...){
      dest_parts = private$.s3_split_path(dest)
      if(isFALSE(overwrite) & self$file_exists(dest))
        stop("File already exists and overwrite is set to `FALSE`", call. = FALSE)
      out = retry_api_call(
        self$s3_client$put_object(
          Bucket = dest_parts$Bucket,
          Key = dest_parts$Key,
          Body = obj,
          ...
        ), self$retries)
      self$clear_cache(dest)
    },

    # Using 100 MB multipart upload size due to AWS recommendation:
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
    .s3_stream_out_multipart_file = function(obj,
                                             dest,
                                             size,
                                             max_batch = 100 * MB,
                                             overwrite = FALSE,
                                             ...){
      dest_parts = private$.s3_split_path(dest)
      if(isFALSE(overwrite) & self$file_exists(dest))
        stop("File already exists and overwrite is set to `FALSE`", call. = FALSE)
      upload_id = retry_api_call(
        self$s3_client$create_multipart_upload(
          Bucket = dest_parts$Bucket, Key = dest_parts$Key, ...
        )$UploadId, self$retries)
      num_parts = ceiling(size / max_batch)

      kwargs = list(...)
      kwargs$Bucket = dest_parts$Bucket
      kwargs$Key = dest_parts$Key
      kwargs$UploadId = upload_id
      tryCatch({
        parts = future_lapply(seq_len(num_parts), function(i){
            kwargs$PartNumber = i
            kwargs$Body = obj[[i]]
            etag = retry_api_call(
              do.call(self$s3_client$upload_part, kwargs)$ETag,
              self$retries
            )
            return(list(ETag = etag, PartNumber = i))
          },
          future.seed = num_parts
        )
        kwargs$MultipartUpload = list(Parts = parts)
        retry_api_call(
          do.call(self$s3_client$complete_multipart_upload, kwargs),
          self$retries
        )
      },
      error = function(cond){
        kwargs$MultipartUpload = NULL
        retry_api_call(
          do.call(self$s3_client$abort_multipart_upload, kwargs),
          self$retries
        )
        LOGGER$error("Failed to Upload file in Multiparts")
        stop(cond)
      })
      self$clear_cache(dest)
    },

    .s3_stream_out_url = function(obj,
                                  dest,
                                  max_batch = 100 * MB,
                                  overwrite = FALSE,
                                  ...){
      dest_parts = private$.s3_split_path(dest)
      if(isFALSE(overwrite) & self$file_exists(dest))
        stop("File already exists and overwrite is set to `FALSE`", call. = FALSE)
      upload_id = retry_api_call(
        self$s3_client$create_multipart_upload(
          Bucket = dest_parts$Bucket, Key = dest_parts$Key, ...
        )$UploadId, self$retries)

      kwargs = list(...)
      kwargs$Bucket = dest_parts$Bucket
      kwargs$Key = dest_parts$Key
      kwargs$UploadId = upload_id
      stream = curl::curl(obj)
      open(stream, "rbf")
      on.exit(close(stream))
      max_batch = as.numeric(max_batch)

      multipart = TRUE
      upload_no = 1
      upload_parts = list()
      while(isIncomplete(stream)) {
        buf = readBin(stream, raw(), max_batch)
        buf_len = length(buf)
        if(buf_len == 0) {
          break
        }
        kwargs$Body = buf
        if(buf_len < 5 *MB & upload_no == 1){
          kwargs$UploadId = NULL
          multipart = FALSE
          retry_api_call(
            do.call(self$s3_client$put_object, kwargs),
            self$retries
          )
          break
        } else {
          tryCatch({
            kwargs$PartNumber = upload_no
            etag = retry_api_call(
              do.call(self$s3_client$upload_part, kwargs)$ETag,
              self$retries
            )
            upload_parts[[upload_no]] = list(
              ETag = etag, PartNumber = upload_no
            )
            upload_no = upload_no + 1
          }, error = function(cond){
            kwargs$MultipartUpload = NULL
            retry_api_call(
              do.call(self$s3_client$abort_multipart_upload, kwargs),
              self$retries
            )
            LOGGER$error("Failed to Upload file in Multiparts")
            stop(cond)
          })
        }
      }
      if (multipart){
        kwargs$PartNumber = NULL
        kwargs$Body = NULL
        kwargs$MultipartUpload = list(Parts = upload_parts)
        retry_api_call(
          do.call(self$s3_client$complete_multipart_upload, kwargs),
          self$retries
        )
      }
      self$clear_cache(dest)
    },

    .s3_upload_standard_file = function(src,
                                        dest,
                                        size,
                                        overwrite = FALSE,
                                        ...){
      dest_parts = private$.s3_split_path(dest)
      if(isFALSE(overwrite) & self$file_exists(dest))
        stop("File already exists and overwrite is set to `FALSE`", call. = FALSE)
      out = retry_api_call(
        self$s3_client$put_object(
          Bucket = dest_parts$Bucket,
          Key = dest_parts$Key,
          Body = readBin(src, what = "raw", n = size),
          ...
        ), self$retries)
      self$clear_cache(dest)
    },

    # Using 100 MB multipart upload size due to AWS recommendation:
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
    .s3_upload_multipart_file = function(src,
                                         dest,
                                         size,
                                         max_batch = 100 * MB,
                                         overwrite = FALSE,
                                         ...){
      dest_parts = private$.s3_split_path(dest)
      if(isFALSE(overwrite) & self$file_exists(dest))
        stop("File already exists and overwrite is set to `FALSE`", call. = FALSE)

      LOGGER$debug(
        "Uploading file '%s' in multipart to: '%s'", src, dest
      )
      upload_id = retry_api_call(
        self$s3_client$create_multipart_upload(
          Bucket = dest_parts$Bucket, Key = dest_parts$Key, ...
        )$UploadId, self$retries)
      max_batch = as.numeric(max_batch)
      num_parts = ceiling(size / max_batch)
      con = file(src, open = "rb")
      on.exit({close(con)})

      kwargs = list(...)
      kwargs$Bucket = dest_parts$Bucket
      kwargs$Key = dest_parts$Key
      kwargs$UploadId = upload_id
      tryCatch({
        parts = lapply(seq_len(num_parts), function(i){
          kwargs$Body = readBin(con, what = "raw", n = max_batch)
          kwargs$PartNumber = i
          etag = retry_api_call(
            do.call(self$s3_client$upload_part, kwargs)$ETag,
            self$retries
          )
          return(list(ETag = etag, PartNumber = i))
        })
        kwargs$MultipartUpload = list(Parts = parts)
        kwargs$Body = NULL
        kwargs$PartNumber = NULL
        retry_api_call(
          do.call(self$s3_client$complete_multipart_upload, kwargs),
          self$retries
        )
      },
      error = function(cond){
        kwargs$MultipartUpload = NULL
        kwargs$Body = NULL
        kwargs$PartNumber = NULL
        retry_api_call(
          do.call(self$s3_client$abort_multipart_upload, kwargs),
          self$retries
        )
        LOGGER$error("Failed to Upload file in Multiparts")
        stop(cond)
      })
      self$clear_cache(dest)
    },

    .s3_pnt_dir = function(path){
      path = unname(vapply(path, private$.s3_strip_uri, FUN.VALUE = ""))
      if(any(grepl("/", path))) {
        pnt = dirname(path)[dirname(path) !="."]
        return(pnt)
      } else {
       return("")
      }
    },

    .s3_strip_uri = function(path = "", dir = F){
      s3_protocol = "s3://"
      if (startsWith(tolower(path), s3_protocol)){
        path = substr(path, nchar(s3_protocol) + 1, nchar(path))
      }
      if(!dir)
        path = trimws(path, which = "right", whitespace = "/")
      return (if(!is.na(path)) path else self$root_marker)
    },

    .s3_build_uri = function(parts){
      parts = lapply(Filter(Negate(is.null), parts), function(x) enc2utf8(as.character(x)))
      return(vapply(parts, function(x) {
        paste("s3:/", paste(trimws(gsub("s3://", "", x), whitespace="/"), collapse = "/"), sep = "/")
      }, FUN.VALUE = "")
      )
    },

    .s3_split_path = function(path) {
      path = trimws(path, which = "left", "/")
      if (!grepl("/", path)) {
        return(list(Bucket = path, Key = "", VersionId = NULL))
      } else {
        parts = str_split(path, "/", n = 2, fixed = T)[[1]]
        keyparts = str_split(parts[2], "?versionId=", fixed = T)[[1]]
        return(list(
          Bucket = parts[1],
          Key = keyparts[1],
          VersionId = if(is.na(keyparts[2])) NULL else keyparts[2]
        ))
      }
    },

    .cred_set = function(aws_access_key_id,
                         aws_secret_access_key,
                         aws_session_token,
                         profile_name,
                         region_name,
                         endpoint,
                         disable_ssl,
                         anonymous,
                         ...){
      add_list = function(x) if(length(x) == 0) NULL else x
      config = list()
      credentials = list()
      cred = list()

      cred$access_key_id = aws_access_key_id
      cred$secret_access_key = aws_secret_access_key
      cred$session_token = aws_session_token

      credentials$creds = add_list(cred)
      credentials$profile = profile_name
      credentials$anonymous = anonymous
      config$credentials = add_list(credentials)
      config$region = region_name
      config$endpoint = endpoint
      config$disable_ssl = disable_ssl

      return(modifyList(config, list(...)))
    }
  )
)
