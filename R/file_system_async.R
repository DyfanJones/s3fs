#' @include s3filesystem_class.R

#' @import future

############################################################################
# File methods
############################################################################

#' @title Copy files and directories
#' @description
#' `s3_file_copy` copies files
#'
#' `s3_dir_copy` copies the directory recursively to the new location
#' @param path (character): path to a local directory of file or a uri.
#' @param new_path (character): path to a local directory of file or a uri.
#' @param max_batch (\link[fs]{fs_bytes}): Maximum batch size being uploaded with each multipart.
#' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
#'              and the file exists an error will be thrown.
#' @param ... parameters to be passed to \code{\link[paws.storage]{s3_put_object}}
#' @return return \code{\link[future]{future}} object of [s3_file_copy()], [s3_dir_copy()]
#' @seealso \code{\link[future]{future}} [s3_file_copy()] [s3_dir_copy()]
#' @name copy_async
#' @export
s3_file_copy_async = function(path,
                              new_path,
                              max_batch = fs_bytes("100MB"),
                              overwrite = FALSE,
                              ...){
  s3fs = s3_file_system()
  return(future({s3fs$file_copy(path, new_path, max_batch, overwrite, ...)}))
}

#' @title Delete files and directories
#' @description
#' `s3_file_delete` delete files in AWS S3
#'
#' `s3_dir_delete` delete directories in AWS S3 recursively.
#' @param path (character): A character vector of paths or s3 uris.
#' @param ... parameters to be passed to \code{\link[paws.storage]{s3_delete_objects}}
#' @return return \code{\link[future]{future}} object of [s3_file_delete()] [s3_dir_delete()]
#' @seealso \code{\link[future]{future}} [s3_file_delete()] [s3_dir_delete()]
#' @name delete_async
#' @export
s3_file_delete_async = function(path, ...){
  s3fs = s3_file_system()
  return(future({s3fs$file_delete(path, ...)}))
}

#' @title Download files and directories
#' @description
#' `s3_file_download` downloads `AWS S3` files to local
#'
#' `s3_file_download` downloads `AWS s3` directory to local
#' @param path (character): A character vector of paths or uris
#' @param new_path (character): A character vector of paths to the new locations.
#' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
#'              and the file exists an error will be thrown.
#' @param ... parameters to be passed to \code{\link[paws.storage]{s3_get_object}}
#' @return return \code{\link[future]{future}} object of [s3_file_download()] [s3_dir_download()]
#' @seealso \code{\link[future]{future}} [s3_file_download()] [s3_dir_download()]
#' @name download_async
#' @export
s3_file_download_async = function(path,
                                  new_path,
                                  overwrite=FALSE,
                                  ...){
  s3fs = s3_file_system()
  return(future({s3fs$file_download(path, new_path, overwrite, ...)}))
}


#' @title Move or rename S3 files
#' @description Move files to another location on AWS S3
#' @param path (character): A character vector of s3 uri
#' @param new_path (character): A character vector of s3 uri.
#' @param max_batch (numeric): Maximum batch size being uploaded with each multipart.
#' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
#'              and the file exists an error will be thrown.
#' @param ... parameters to be passed to \code{\link[paws.storage]{s3_copy_object}}
#' @return return \code{\link[future]{future}} object of [s3_file_move()]
#' @seealso \code{\link[future]{future}} [s3_file_move()]
#' @export
s3_file_move_async = function(path,
                        new_path,
                        max_batch = 100*MB,
                        overwrite = FALSE,
                        ...){
  s3fs = s3_file_system()
  return(future({s3fs$file_move(path, new_path, max_batch, overwrite, ...)}))
}

#' @title Streams data from R to AWS S3.
#' @description
#' `s3_file_stream_in` streams in AWS S3 file as a raw vector
#'
#' `s3_file_stream_out` streams raw vector out to AWS S3 file
#' @param path (character): A character vector of paths or s3 uri
#' @param obj (raw|character): A raw vector, rawConnection, url to be streamed up to AWS S3.
#' @param max_batch (\link[fs]{fs_bytes}): Maximum batch size being uploaded with each multipart.
#' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
#'              and the file exists an error will be thrown.
#' @param ... parameters to be passed to \code{\link[paws.storage]{s3_get_object}} and
#'              \code{\link[paws.storage]{s3_put_object}}
#' @return return \code{\link[future]{future}} object of [s3_file_stream_in()] [s3_file_stream_out()]
#' @seealso \code{\link[future]{future}} [s3_file_move()] [s3_file_stream_in()] [s3_file_stream_out()]
#' @name stream_async
#' @export
s3_file_stream_in_async = function(path,
                                   ...){
  s3fs = s3_file_system()
  return(future({s3fs$file_stream_in(path, ...)}))
}

#' @rdname stream_async
#' @export
s3_file_stream_out_async = function(obj,
                                    path,
                                    max_batch = fs_bytes("100MB"),
                                    overwrite = FALSE,
                                    ...){
  s3fs = s3_file_system()
  return(future({s3fs$file_stream_out(obj, path, max_batch, overwrite, ...)}))
}

#' @title Upload file and directory
#' @description
#' `s3_file_upload` upload files to AWS S3
#'
#' `s3_dir_upload` upload directory to AWS S3
#' @param path (character): A character vector of local file paths to upload to AWS S3
#' @param new_path (character): A character vector of AWS S3 paths or uri's of the new locations.
#' @param max_batch (\link[fs]{fs_bytes}): Maximum batch size being uploaded with each multipart.
#' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
#'              and the file exists an error will be thrown.
#' @param ... parameters to be passed to \code{\link[paws.storage]{s3_put_object}}
#'              and \code{\link[paws.storage]{s3_create_multipart_upload}}
#' @return return \code{\link[future]{future}} object of [s3_file_upload()] [s3_dir_upload()]
#' @seealso \code{\link[future]{future}} [s3_file_move()] [s3_file_upload()] [s3_dir_upload()]
#' @name upload_async
#' @export
s3_file_upload_async = function(path,
                                new_path,
                                max_batch = fs_bytes("100MB"),
                                overwrite = FALSE,
                                ...){
  s3fs = s3_file_system()
  return(future({s3fs$file_upload(path, new_path, max_batch, overwrite, ...)}))
}

############################################################################
# Directory methods
############################################################################

#' @rdname copy_async
#' @export
s3_dir_copy_async = function(path,
                             new_path,
                             max_batch = fs_bytes("100MB"),
                             overwrite = FALSE,
                             ...){
  s3fs = s3_file_system()
  return(future({s3fs$dir_copy(path, new_path, max_batch, overwrite, ...)}))
}

#' @rdname delete_async
#' @export
s3_dir_delete_async = function(path){
  s3fs = s3_file_system()
  return(future({s3fs$dir_delete(path)}))
}

#' @rdname download_async
#' @export
s3_dir_download_async = function(path,
                                 new_path,
                                 overwrite = FALSE,
                                 ...){
  s3fs = s3_file_system()
  return(future({s3fs$dir_download(path, new_path, overwrite, ...)}))
}

#' @rdname upload_async
#' @export
s3_dir_upload_async = function(path,
                               new_path,
                               max_batch,
                               overwrite = FALSE,
                               ...){
  s3fs = s3_file_system()
  return(future({s3fs$dir_upload(path, new_path, max_batch, overwrite, ...)}))
}
