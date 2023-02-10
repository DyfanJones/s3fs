bucket_nv = Sys.getenv("AWS_S3_BUCKET_NOT_VERSIONED")

test_that("check is s3 uri is file", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  file_path = s3_file_create(s3_file_temp(tmp_dir = bucket_nv))
  dir_path = s3_dir_create(s3_file_temp(tmp_dir = bucket_nv))

  file_result = s3_is_file(file_path)
  dir_result = s3_is_file(dir_path)

  s3_file_delete(file_path)
  s3_dir_delete(dir_path)

  expect_true(file_result)
  expect_false(dir_result)
})

test_that("check is s3 uri is directory", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  file_path = s3_file_create(s3_file_temp(tmp_dir = bucket_nv))
  dir_path = s3_dir_create(s3_file_temp(tmp_dir = bucket_nv))

  file_result = s3_is_dir(file_path)
  dir_result = s3_is_dir(dir_path)

  s3_file_delete(file_path)
  s3_dir_delete(dir_path)

  expect_false(file_result)
  expect_true(dir_result)
})

test_that("check is s3 uri is bucket", {
  skip_if_no_env()
  s3fs = s3_file_system(refresh = T)

  buckets = c(
    bucket_nv, # account bucket
    s3_file_temp(tmp_dir = bucket_nv), # made up object
    "s3://voltrondata-labs-datasets", # non-account bucket
    "s3://made-up"# non-account made up bucket
  )
  result = s3_is_bucket(buckets)

  expect_equal(result, c(T,F,T,F))
})

test_that("check is s3 uri is bucket", {
  skip_if_no_env()
  s3fs = s3_file_system(refresh = T)

  path = s3_file_create(s3_file_temp(tmp_dir = bucket_nv))

  result1 = s3_is_file_empty(path)
  result2 = s3_is_file_empty(s3_path(bucket_nv, "multipart_file.csv"))

  expect_true(result1)
  expect_false(result2)
})
