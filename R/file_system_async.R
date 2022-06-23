#' @include s3filesystem_class.R

#' @import future

############################################################################
# File methods
############################################################################

#' @rdname permission
#' @export
s3_file_chmod_async = function(path,
                               mode = c(
                                 'private',
                                 'public-read',
                                 'public-read-write',
                                 'authenticated-read',
                                 'aws-exec-read',
                                 'bucket-owner-read',
                                 'bucket-owner-full-control')){
  s3fs = s3_file_system()
  return(future({s3fs$file_chmod(path, mode)}))
}

#' @rdname copy
#' @export
s3_file_copy_async = function(path,
                              new_path,
                              max_batch = 100 * MB,
                              overwrite = FALSE,
                              ...){
  s3fs = s3_file_system()
  return(future({s3fs$file_copy(path, new_path, max_batch, overwrite, ...)}))
}

#' @rdname delete
#' @export
s3_file_delete_async = function(path, ...){
  s3fs = s3_file_system()
  return(future({s3fs$file_delete(path, ...)}))
}

#' @rdname download
#' @export
s3_file_download_async = function(path,
                                  new_path,
                                  overwrite=FALSE,
                                  ...){
  s3fs = s3_file_system()
  return(future({s3fs$file_download(path, new_path, overwrite, ...)}))
}

#' @rdname info
#' @export
s3_file_info_async = function(path){
  s3fs = s3_file_system()
  return(future({s3fs$file_info(path)}))
}

#' @rdname stream
#' @export
s3_file_stream_in_async = function(path,
                                   ...){
  s3fs = s3_file_system()
  return(future({s3fs$file_stream_in(path, ...)}))
}

#' @rdname stream
#' @export
s3_file_stream_out_async = function(obj,
                                    path,
                                    max_batch = 100 * MB,
                                    overwrite = FALSE,
                                    ...){
  s3fs = s3_file_system()
  return(future({s3fs$file_stream_out(obj, path, max_batch, overwrite, ...)}))
}

#' @rdname touch
#' @export
s3_file_touch_async = function(path,
                               ...){
  s3fs = s3_file_system()
  return(future({s3fs$file_touch(path, ...)}))
}

#' @rdname upload
#' @export
s3_file_upload_async = function(path,
                                new_path,
                                max_batch = 100 * MB,
                                overwrite = FALSE,
                                ...){
  s3fs = s3_file_system()
  return(future({s3fs$file_upload(path, new_path, max_batch, overwrite, ...)}))
}

############################################################################
# Directory methods
############################################################################

#' @rdname copy
#' @export
s3_dir_copy_async = function(path,
                             new_path,
                             max_batch = 100 * MB,
                             overwrite = FALSE,
                             ...){
  s3fs = s3_file_system()
  return(future({s3fs$dir_copy(path, new_path, max_batch, overwrite, ...)}))
}

#' @rdname delete
#' @export
s3_dir_delete_async = function(path){
  s3fs = s3_file_system()
  return(future({s3fs$dir_delete(path)}))
}

#' @rdname download
#' @export
s3_dir_download_async = function(path,
                                 new_path,
                                 overwrite = FALSE,
                                 ...){
  s3fs = s3_file_system()
  return(future({s3fs$dir_download(path, new_path, overwrite, ...)}))
}

#' @rdname info
#' @export
s3_dir_info_async = function(path = ".",
                             type = c("any", "bucket", "directory", "file"),
                             glob = NULL,
                             regexp = NULL,
                             invert = FALSE,
                             recurse = FALSE,
                             refresh = FALSE){
  s3fs = s3_file_system()
  return(future({s3fs$dir_info(path, type, glob, regexp, invert, recurse, refresh)}))
}

#' @rdname info
#' @export
s3_dir_ls_async = function(path = ".",
                           type = c("any", "bucket", "directory", "file"),
                           glob = NULL,
                           regexp = NULL,
                           invert = FALSE,
                           recurse = FALSE,
                           refresh = FALSE){
  s3fs = s3_file_system()
  return(future({s3fs$dir_ls(path, type, glob, regexp, invert, recurse, refresh)}))
}

#' @rdname upload
#' @export
s3_dir_upload_async = function(path,
                               new_path,
                               max_batch,
                               overwrite = FALSE,
                               ...){
  s3fs = s3_file_system()
  return(future({s3fs$dir_upload(path, new_path, max_batch, overwrite, ...)}))
}
