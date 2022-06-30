
test_that("check bucket creation", {
  skip_if_no_env()
  path = s3_bucket_create(s3_file_temp("bucket"))

  result = s3_dir_ls()

  s3_bucket_delete(path)

  expect_true(path %in% result)
})
