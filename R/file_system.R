#' @include s3filesystem_class.R

s3fs_cache = new.env(parent = emptyenv())

#' @title Access AWS S3 as if it were a file system.
#' @description This creates a file system "like" API based off \code{fs}
#'              (e.g. dir_ls, file_copy, etc.) for AWS S3 storage.
#' @param aws_access_key_id (character): AWS access key ID
#' @param aws_secret_access_key (character): AWS secret access key
#' @param aws_session_token (character): AWS temporary session token
#' @param region_name (character): Default region when creating new connections
#' @param profile_name (character): The name of a profile to use. If not given,
#'              then the default profile is used.
#' @param endpoint (character): The complete URL to use for the constructed client.
#' @param disable_ssl (logical): Whether or not to use SSL. By default, SSL is used.
#' @param refresh (logical): Refresh cached S3FileSystem class
#' @param ... Other parameters within \code{paws} client.
#' @export
s3_file_system = function(aws_access_key_id = NULL,
                          aws_secret_access_key = NULL,
                          aws_session_token = NULL,
                          region_name = NULL,
                          profile_name = NULL,
                          endpoint = NULL,
                          disable_ssl = FALSE,
                          refresh = FALSE,
                          ...){
  s3fs = NULL
  if(!refresh){
    s3fs = tryCatch({
      get(service, envir = s3fs_cache, inherits = FALSE)
    }, error = function(e) NULL
    )
  }
  if (is.null(s3fs))
    s3fs = S3FileSystem$new(
      aws_access_key_id = aws_access_key_id,
      aws_secret_access_key = aws_secret_access_key,
      aws_session_token = aws_session_token,
      region_name = region_name,
      profile_name = profile_name,
      endpoint = endpoint,
      ...
    )
  assign("service", s3fs, envir = s3fs_cache)
  return(invisible(s3fs))
}

############################################################################
# File methods
############################################################################

#' @title Change file permissions
#' @param path (character): A character vector of path or s3 uri.
#' @param mode (character): A character of the mode
#' @name permission
#' @export
s3_file_chmod = function(path,
                         mode = c(
                           'private',
                           'public-read',
                           'public-read-write',
                           'authenticated-read',
                           'aws-exec-read',
                           'bucket-owner-read',
                           'bucket-owner-full-control')){
  s3fs = s3_file_system()
  return(s3fs$file_chmod(path, mode))
}

#' @title Copy files and directories
#' @description
#' `s3_file_copy` copies files
#'
#' `s3_dir_copy` copies the directory recursively to the new location
#' @param path (character): path to a local directory of file or a uri.
#' @param new_path (character): path to a local directory of file or a uri.
#' @param max_batch (numeric): Maximum batch size being uploaded with each multipart.
#' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
#'              and the file exists an error will be thrown.
#' @param ... parameters to be passed to \code{\link[paws.storage]{s3_put_object}}
#' @name copy
#' @export
s3_file_copy = function(path,
                        new_path,
                        max_batch = 100 * MB,
                        overwrite = FALSE,
                        ...){
  s3fs = s3_file_system()
  return(s3fs$file_copy(path, new_path, max_batch, overwrite, ...))
}

#' @title Create files and directories
#' @description
#' `s3_file_create` create file on `AWS S3`, if file already exists it will be left unchanged.
#'
#' `s3_dir_create` create empty directory of `AWS S3`.
#' @param path (character): A character vector of path or s3 uri.
#' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
#'              and the file exists an error will be thrown.
#' @param ... parameters to be passed to \code{\link[paws.storage]{s3_put_object}}
#' @name create
#' @export
s3_file_create = function(path,
                          overwrite = FALSE,
                          ...){
  s3fs = s3_file_system()
  return(s3fs$file_create(path, overwrite, ...))
}

#' @title Delete files and directories
#' @description
#' `s3_file_delete` delete files in AWS S3
#'
#' `s3_dir_delete` delete directories in AWS S3 recursively.
#' @param path (character): A character vector of paths or s3 uris.
#' @param ... parameters to be passed to \code{\link[paws.storage]{s3_delete_objects}}
#' @name delete
#' @export
s3_file_delete = function(path, ...){
  s3fs = s3_file_system()
  return(s3fs$file_delete(path, ...))
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
#' @name download
#' @export
s3_file_download = function(path,
                            new_path,
                            overwrite=FALSE,
                            ...){
  s3fs = s3_file_system()
  return(s3fs$file_download(path, new_path, overwrite, ...))
}

#' @title Download files and directories
#' @description
#' `s3_file_exists` check if file exists in AWS S3
#'
#' `s3_dir_exists` check if path is a directory in AWS S3
#' @param path (character) s3 path to check
#' @name exists
#' @export
s3_file_exists = function(path){
  s3fs = s3_file_system()
  return(s3fs$file_exists(path))
}

#' @title Get files and directories information
#' @description
#' `s3_file_info` returns file information within AWS S3 directory
#'
#' `s3_dir_info` returns file name information within AWS S3 directory
#'
#' `s3_dir_ls` returns file name within AWS S3 directory
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
#' @name info
#' @export
s3_file_info = function(path){
  s3fs = s3_file_system()
  return(s3fs$file_info(path))
}

#' @title Move or rename S3 files
#' @description Move files to another location on AWS S3
#' @param path (character): A character vector of s3 uri
#' @param new_path (character): A character vector of s3 uri.
#' @param max_batch (numeric): Maximum batch size being uploaded with each multipart.
#' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
#'              and the file exists an error will be thrown.
#' @param ... parameters to be passed to \code{\link[paws.storage]{s3_copy_object}}
#' @export
s3_file_move = function(path,
                        new_path,
                        max_batch = 100*MB,
                        overwrite = FALSE,
                        ...){
  s3fs = s3_file_system()
  return(s3fs$file_move(path, new_path, max_batch, overwrite, ...))
}

#' @title Streams data from R to AWS S3.
#' @description
#' `s3_file_stream_in` streams in AWS S3 file as a raw vector
#'
#' `s3_file_stream_out` streams raw vector out to AWS S3 file
#' @param path (character): A character vector of paths or s3 uri
#' @param obj (raw): A raw vector or rawConnection to be streamed up to AWS S3.
#' @param max_batch (numeric): Maximum batch size being uploaded with each multipart.
#' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
#'              and the file exists an error will be thrown.
#' @param ... parameters to be passed to \code{\link[paws.storage]{s3_get_object}} and
#'              \code{\link[paws.storage]{s3_put_object}}
#' @name stream
#' @export
s3_file_stream_in = function(path,
                             ...){
  s3fs = s3_file_system()
  return(s3fs$file_stream_in(path, ...))
}

#' @rdname stream
#' @export
s3_file_stream_out = function(obj,
                             path,
                             max_batch = 100 * MB,
                             overwrite = FALSE,
                             ...){
  s3fs = s3_file_system()
  return(s3fs$file_stream_out(obj, path, max_batch, overwrite, ...))
}


#' @title Create name for temporary files
#' @description return the name which can be used as a temporary file
#' @param pattern (character): A character vector with the non-random portion of the name.
#' @param tmp_dir (character): The directory the file will be created in. By default
#'              the cached s3 bucket will be applied otherwise \code{""} will be used.
#' @param ext (character): A character vector of one or more paths.
#' @export
s3_file_temp = function(pattern = "file",
                        tmp_dir = "",
                        ext = ""){
  s3fs = s3_file_system()
  return(s3fs$file_temp(pattern, tmp_dir, ext))
}


#' @title Change file modification time
#' @description Similar to `fs::file_touch` this does not create the file if
#'              it does not exist. Use `s3fs$file_create()` to do this if needed.
#' @param path (character): A character vector of paths or s3 uri
#' @param ... parameters to be passed to \code{\link[paws.storage]{s3_copy_object}}
#' @note This method will only update the modification time of the AWS S3 object.
#' @name touch
#' @export
s3_file_touch = function(path,
                         ...){
  s3fs = s3_file_system()
  return(s3fs$file_touch(path, ...))
}

#' @title Upload file and directory
#' @description
#' `s3_file_upload` upload files to AWS S3
#'
#' `s3_dir_upload` upload directory to AWS S3
#' @param path (character): A character vector of local file paths to upload to AWS S3
#' @param new_path (character): A character vector of AWS S3 paths or uri's of the new locations.
#' @param max_batch (numeric): Maximum batch size being uploaded with each multipart.
#' @param overwrite (logical): Overwrite files if the exist. If this is \code{FALSE}
#'              and the file exists an error will be thrown.
#' @param ... parameters to be passed to \code{\link[paws.storage]{s3_put_object}}
#'              and \code{\link[paws.storage]{s3_create_multipart_upload}}
#' @name upload
#' @export
s3_file_upload = function(path,
                          new_path,
                          max_batch = 100 * MB,
                          overwrite = FALSE,
                          ...){
  s3fs = s3_file_system()
  return(s3fs$file_upload(path, new_path, max_batch, overwrite, ...))
}

#' @title Modifying file tags
#' @description
#' `s3_file_tag_delete` delete file tags
#'
#' `s3_file_tag_info` get file tags
#'
#' `s3_file_tag_info`
#' @param path (character): A character vector of paths or s3 uri
#' @param tags (list): Tags to be applied
#' @param overwrite (logical): To overwrite tagging or to modify inplace. Default will
#'             modify inplace.
#' @name tag
#' @export
s3_file_tag_delete = function(path){
  s3fs = s3_file_system()
  return(s3fs$file_tag_delete(path, ...))
}

#' @rdname tag
#' @export
s3_file_tag_info = function(path){
  s3fs = s3_file_system()
  return(s3fs$file_tag_info(path))
}

#' @rdname tag
#' @export
s3_file_tag_update = function(path,
                              tags,
                              overwrite=FALSE){
  s3fs = s3_file_system()
  return(s3fs$file_tag_update(path, tags, overwrite))
}

#' @title Query file version metadata
#' @description Get file versions
#' @param path (character): A character vector of paths or uris
#' @param ... parameters to be passed to \code{\link[paws.storage]{s3_list_object_versions}}
#' @export
s3_file_version_info = function(path,
                                ...){
  s3fs = s3_file_system()
  return(s3fs$file_version_info(path, ...))
}

############################################################################
# Test File methods
############################################################################
#' @title Functions to test for file types
#' @description Test for file types
#' @param path (character): A character vector of paths or uris
#' @name file_type
#' @export
s3_is_file = function(path){
  s3fs = s3_file_system()
  return(s3fs$is_file(path))
}

#' @rdname file_type
#' @export
s3_is_dir = function(path){
  s3fs = s3_file_system()
  return(s3fs$is_dir(path))
}

#' @rdname file_type
#' @export
s3_is_bucket = function(path){
  s3fs = s3_file_system()
  return(s3fs$is_bucke(path))
}

#' @rdname file_type
#' @export
s3_is_file_empty = function(path){
  s3fs = s3_file_system()
  return(s3fs$is_file_empty(path))
}

############################################################################
# Directory methods
############################################################################

#' @rdname copy
#' @export
s3_dir_copy = function(path,
                       new_path,
                       max_batch = 100 * MB,
                       overwrite = FALSE,
                       ...){
  s3fs = s3_file_system()
  return(s3fs$dir_copy(path, new_path, max_batch, overwrite, ...))
}

#' @rdname create
#' @export
s3_dir_create = function(path,
                         overwrite=FALSE,
                         ...){
  s3fs = s3_file_system()
  return(s3fs$dir_create(path, overwrite, ...))
}

#' @rdname delete
#' @export
s3_dir_delete = function(path){
  s3fs = s3_file_system()
  return(s3fs$dir_delete(path))
}

#' @rdname exists
#' @export
s3_dir_exists = function(path = "."){
  s3fs = s3_file_system()
  return(s3fs$dir_exists(path))
}

#' @rdname download
#' @export
s3_dir_download = function(path,
                           new_path,
                           overwrite = FALSE,
                           ...){
  s3fs = s3_file_system()
  return(s3fs$dir_download(path, new_path, overwrite, ...))
}

#' @rdname info
#' @export
s3_dir_info = function(path = ".",
                       type = c("any", "bucket", "directory", "file"),
                       glob = NULL,
                       regexp = NULL,
                       invert = FALSE,
                       recurse = FALSE,
                       refresh = FALSE){
  s3fs = s3_file_system()
  return(s3fs$dir_info(path, type, glob, regexp, invert, recurse, refresh))
}

#' @rdname info
#' @export
s3_dir_ls = function(path = ".",
                     type = c("any", "bucket", "directory", "file"),
                     glob = NULL,
                     regexp = NULL,
                     invert = FALSE,
                     recurse = FALSE,
                     refresh = FALSE){
  s3fs = s3_file_system()
  return(s3fs$dir_ls(path, type, glob, regexp, invert, recurse, refresh))
}

#' @rdname upload
#' @export
s3_dir_upload = function(path,
                         new_path,
                         max_batch,
                         overwrite = FALSE,
                         ...){
  s3fs = s3_file_system()
  return(s3fs$dir_upload(path, new_path, max_batch, overwrite, ...))
}

############################################################################
# Path methods
############################################################################

#' @title Construct path for file or directory
#' @description Constructs a s3 uri path
#' @param ... (character): Character vectors
#' @param ext (character): An optional extension to append to the generated path
#' @name path
#' @export
s3_path = function(..., ext = ""){
  s3fs = s3_file_system()
  return(s3fs$path(..., ext=ext))
}

#' @title Manipulate s3 uri paths
#' @description
#' `s3_path_dir` returns the directory portion of s3 uri
#'
#' `s3_path_file` returns the file name portion of the s3 uri path
#'
#' `s3_path_ext` returns the last extension for a path.
#'
#' `s3_path_ext_remove` removes the last extension and return the rest of the s3 uri.
#'
#' `s3_path_ext_set` replace the extension with a new extension.
#' @param path (character): A character vector of paths
#' @param ext (character): New file extension
#' @name path_manipulate
#' @export
s3_path_dir = function(path){
  s3fs = s3_file_system()
  return(s3fs$path_dir(...))
}

#' @name path_manipulate
#' @export
s3_path_file = function(path){
  s3fs = s3_file_system()
  return(s3fs$path_file(path))
}

#' @name path_manipulate
#' @export
s3_path_ext = function(path){
  s3fs = s3_file_system()
  return(s3fs$path_ext(path))
}

#' @name path_manipulate
#' @export
s3_path_ext_remove = function(path){
  s3fs = s3_file_system()
  return(s3fs$path_ext_remove(path))
}

#' @name path_manipulate
#' @export
s3_path_ext_set = function(path,
                           ext){
  s3fs = s3_file_system()
  return(s3fs$path_ext_set(path, ext))
}

#' @title Construct AWS S3 path
#' @description Construct an s3 uri path from path vector
#' @param path (character): A character vector of one or more paths
#' @export
s3_path_join = function(path){
  s3fs = s3_file_system()
  return(s3fs$path_join(path))
}

#' @title Split s3 path and uri
#' @description Split s3 uri path to core components bucket, key and version id
#' @param path (character): A character vector of one or more paths or s3 uri
#' @export
s3_path_split = function(path){
  s3fs = s3_file_system()
  return(s3fs$path_split(path))
}
