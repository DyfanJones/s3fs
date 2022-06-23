
<!-- README.md is generated from README.Rmd. Please edit that file -->

# s3fs

`s3fs` provides a file-system like interface into Amazon Web Services
for `R`. It utilizes [`paws`](https://github.com/paws-r/paws) `SDK`,
[`R6`](https://github.com/r-lib/R6). This repo has been inspired by
Python’s [`s3fs`](https://github.com/fsspec/s3fs), however it’s API and
implementation has been developed to follow `R`’s
[`fs`](https://github.com/r-lib/fs).

## Installation

Currently only avialable on github:

``` r
remotes::install_github("dyfanjones/s3fs")
```

# Comparison with `fs`

`s3fs` attempts to give the same interface as `fs` when handling files
on AWS S3 from `R`.

-   Vectorization. All `s3fs` functions are vectorized, accepting
    multiple path inputs similar to `fs`.

## Usage

`fs` has a straight forward API with 4 core themes:

-   `path_` for manipulating and constructing paths
-   `file_` for files
-   `dir_` for directories
-   `link_` for links

`s3fs` follows theses themes with the following:

-   `s3_path_` for manipulating and constructing s3 uri paths
-   `s3_file_` for s3 files
-   `s3_dir_` for s3 directories

**NOTE:** `link_` is currently not supported.

``` r
library(s3fs)

# Construct a path to a file with `path()`
s3_path("foo", "bar", letters[1:3], ext = "txt")
#> [1] "s3://foo/bar/a.txt" "s3://foo/bar/b.txt" "s3://foo/bar/c.txt"

# list buckets
s3_dir_ls()
#> [1] "s3://MyBucket1"
#> [2] "s3://MyBucket2"                                        
#> [3] "s3://MyBucket3"               
#> [4] "s3://MyBucket4"                            
#> [5] "s3://MyBucket5"

# list files in bucket
s3_dir_ls("s3://MyBucket5")
#> [1] "s3://MyBucket5/iris.json"     "s3://MyBucket5/athena-query/"
#> [3] "s3://MyBucket5/data/"         "s3://MyBucket5/default/"     
#> [5] "s3://MyBucket5/iris/"         "s3://MyBucket5/made-up/"     
#> [7] "s3://MyBucket5/test_df/"

# create a new directory
tmp <- s3_dir_create(s3_path("MyBucket5", fs::file_temp()))
tmp
#> [1] "s3://MyBucket5/var/folders/sp/scbzkbwx6hbchmylsx0y52k80000gn/T/RtmpjtH1NO/file160d04e147723"

# create new files in that directory
s3_file_create(s3_path(tmp, "my-file.txt"))
#> [1] "s3://MyBucket5/var/folders/sp/scbzkbwx6hbchmylsx0y52k80000gn/T/RtmpjtH1NO/file160d04e147723/my-file.txt"
s3_dir_ls(tmp)
#> [1] "s3://MyBucket5/var/folders/sp/scbzkbwx6hbchmylsx0y52k80000gn/T/RtmpjtH1NO/file160d04e147723/my-file.txt"

# remove files from the directory
s3_file_delete(s3_path(tmp, "my-file.txt"))
s3_dir_ls(tmp)
#> character(0)

# remove the directory
s3_dir_delete(tmp)
```

<sup>Created on 2022-06-21 by the [reprex
package](https://reprex.tidyverse.org) (v2.0.1)</sup>

Similar to `fs`, `s3fs` is designed to work well with the pipe.

``` r
library(s3fs)
paths <- s3_file_temp(tmp_dir = "MyBucket") |>
 s3_dir_create() |>
 s3_path(letters[1:5]) |>
 s3_file_create()
paths
#> [1] "s3://MyBucket/fileazqpwujaydqg/a"
#> [2] "s3://MyBucket/fileazqpwujaydqg/b"
#> [3] "s3://MyBucket/fileazqpwujaydqg/c"
#> [4] "s3://MyBucket/fileazqpwujaydqg/d"
#> [5] "s3://MyBucket/fileazqpwujaydqg/e"

paths |> s3_file_delete()
#> [1] "s3://MyBucket/fileazqpwujaydqg/a"
#> [2] "s3://MyBucket/fileazqpwujaydqg/b"
#> [3] "s3://MyBucket/fileazqpwujaydqg/c"
#> [4] "s3://MyBucket/fileazqpwujaydqg/d"
#> [5] "s3://MyBucket/fileazqpwujaydqg/e"
```

<sup>Created on 2022-06-22 by the [reprex
package](https://reprex.tidyverse.org) (v2.0.1)</sup>

**NOTE:** all examples have be developed from `fs`.

# Feedback wanted

Please open a Github ticket raising any issues or feature requests.
