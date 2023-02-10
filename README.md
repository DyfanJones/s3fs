
<!-- README.md is generated from README.Rmd. Please edit that file -->

# s3fs

<!-- badges: start -->

[![s3fs status
badge](https://dyfanjones.r-universe.dev/badges/s3fs)](https://dyfanjones.r-universe.dev)
[![R-CMD-check](https://github.com/DyfanJones/s3fs/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/DyfanJones/s3fs/actions/workflows/R-CMD-check.yaml)
[![Codecov test
coverage](https://codecov.io/gh/DyfanJones/s3fs/branch/main/graph/badge.svg)](https://app.codecov.io/gh/DyfanJones/s3fs?branch=main)
<!-- badges: end -->

`s3fs` provides a file-system like interface into Amazon Web Services
for `R`. It utilizes [`paws`](https://github.com/paws-r/paws) `SDK`and
[`R6`](https://github.com/r-lib/R6) for it’s core design. This repo has
been inspired by Python’s [`s3fs`](https://github.com/fsspec/s3fs),
however it’s API and implementation has been developed to follow `R`’s
[`fs`](https://github.com/r-lib/fs).

## Installation

r-universe installation:

``` r
# Enable repository from dyfanjones
options(repos = c(
  dyfanjones = 'https://dyfanjones.r-universe.dev',
  CRAN = 'https://cloud.r-project.org')
)

# Download and install s3fs in R
install.packages('s3fs')
```

Github installation

``` r
remotes::install_github("dyfanjones/s3fs")
```

### Dependencies

- [`paws`](https://github.com/paws-r/paws): connection with AWS S3
- [`R6`](https://github.com/r-lib/R6): Setup core class
- [`data.table`](https://github.com/Rdatatable/data.table): wrangle
  lists into data.frames
- [`fs`](https://github.com/r-lib/fs): file system on local files
- [`lgr`](https://github.com/s-fleck/lgr): set up logging
- [`future`](https://github.com/HenrikBengtsson/future): set up async
  functionality
- [`future.apply`](https://github.com/HenrikBengtsson/future.apply): set
  up parallel looping

# Comparison with `fs`

`s3fs` attempts to give the same interface as `fs` when handling files
on AWS S3 from `R`.

- **Vectorization**. All `s3fs` functions are vectorized, accepting
  multiple path inputs similar to `fs`.
- **Predictable**.
  - Non-async functions return values that convey a path.
  - Async functions return a `future` object of it’s no-async
    counterpart.
  - The only exception will be `s3_stream_in` which returns a list of
    raw objects.
- **Naming conventions**. s3fs functions follows `fs` naming conventions
  with `dir_*`, `file_*` and `path_*` however with the syntax `s3_`
  infront i.e `s3_dir_*`, `s3_file_*` and `s3_path_*` etc.
- **Explicit failure**. Similar to `fs` if a failure happens, then it
  will be raised and not masked with a warning.

# Extra features:

- **Scalable**. All `s3fs` functions are designed to have the option to
  run in parallel through the use of `future` and `future.apply`.

For example: copy a large file from one location to the next.

``` r
library(s3fs)
library(future)

plan("multisession")

s3_file_copy("s3://mybucket/multipart/large_file.csv", "s3://mybucket/new_location/large_file.csv")
```

`s3fs` to copy a large file (\> 5GB) using multiparts, `future` allows
each multipart to run in parallel to speed up the process.

- **Async**. `s3fs` uses `future` to create a few key async functions.
  This is more focused on functions that might be moving large files to
  and from `R` and `AWS S3`.

For example: Copying a large file from `AWS S3` to `R`.

``` r
library(s3fs)
library(future)

plan("multisession")

s3_file_copy_async("s3://mybucket/multipart/large_file.csv", "large_file.csv")
```

## Usage

`fs` has a straight forward API with 4 core themes:

- `path_` for manipulating and constructing paths
- `file_` for files
- `dir_` for directories
- `link_` for links

`s3fs` follows theses themes with the following:

- `s3_path_` for manipulating and constructing s3 uri paths
- `s3_file_` for s3 files
- `s3_dir_` for s3 directories

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
tmp <- s3_dir_create(s3_file_temp(tmp_dir = "MyBucket5"))
tmp
#> [1] "s3://MyBucket5/filezwkcxx9q5562"

# create new files in that directory
s3_file_create(s3_path(tmp, "my-file.txt"))
#> [1] "s3://MyBucket5/filezwkcxx9q5562/my-file.txt"
s3_dir_ls(tmp)
#> [1] "s3://MyBucket5/filezwkcxx9q5562/my-file.txt"

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

### File systems that emulate S3

`s3fs` allows you to connect to file systems that provides an
S3-compatible interface. For example, [MinIO](https://min.io/) offers
high-performance, S3 compatible object storage. You will be able to
connect to your `MinIO` server using `s3fs::s3_file_system`:

``` r
library(s3fs)

s3_file_system(
  aws_access_key_id = "minioadmin",  
  aws_secret_access_key = "minioadmin",
  endpoint = "http://localhost:9000"
)

s3_dir_ls()
#> [1] ""

s3_bucket_create("s3://testbucket")
#> [1] "s3://testbucket"

# refresh cache
s3_dir_ls(refresh = T)
#> [1] "s3://testbucket"

s3_bucket_delete("s3://testbucket")
#> [1] "s3://testbucket"

# refresh cache
s3_dir_ls(refresh = T)
#> [1] ""
```

<sup>Created on 2022-12-14 with [reprex
v2.0.2](https://reprex.tidyverse.org)</sup>

**NOTE:** if you to want change from AWS S3 to Minio in the same R
session, you will need to set the parameter `refresh = TRUE` when
calling `s3_file_system` again. You can use multiple sessions by using
the R6 class `S3FileSystem` directly.

# Feedback wanted

Please open a Github ticket raising any issues or feature requests.
