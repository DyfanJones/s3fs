# s3fs 0.1.7

* Fix files without extensions will be created as a dir/file instead of file (#47) thanks to @pat-s for raising issue

# s3fs 0.1.6

* Suppress warning in `S3FileSystem` triggered from `s3_file_delete()` (#42, @salim-b)

# s3fs 0.1.5

* Fix for `s3_file_download` to allow multiple file paths passed to `new_path` (#34), thanks to @sckott for contribution.
* Fix for `s3_file_info`, convert `logical(0)` to `NA` to correctly build `data.frame` output.
* improve helper function `str_split` performance

# s3fs 0.1.4

* Fix ensure path is returned for already existing directories (#28)
* set R version >= 3.6.0 (#29)

# s3fs 0.1.3

* Fix hard coded max batch size
* Add seed for future to prevent warning message
* Ensure nested directories are removed from class cache

# s3fs 0.1.2

* Hot fix replace `\()` syntax with `function()`

# s3fs 0.1.1

* Update Description to align with cran requirements

# s3fs 0.1.0

* Added a `NEWS.md` file to track changes to the package.
* Initial cran release
