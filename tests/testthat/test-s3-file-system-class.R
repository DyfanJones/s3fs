
test_that("check class creation with default parameters", {
  skip_if_no_env()
  s3fs = s3_file_system(refresh = T)
  expect_equal(s3fs$region_name, Sys.getenv("AWS_REGION"))
  expect_equal(s3fs$retries, 5)
  expect_equal(s3fs$multipart_threshold, fs::as_fs_bytes("2GB"))
})

test_that("check class creation with dummy parameters", {
  skip_if_no_env()
  s3fs = s3_file_system(
    aws_access_key_id = "key",
    aws_secret_access_key = "secret",
    aws_session_token = "token",
    region_name = "region",
    profile_name = "profile",
    endpoint = "endpoint",
    disable_ssl = FALSE,
    multipart_threshold = 2 * MB,
    retries = 2,
    refresh = T
  )
  expect_equal(s3fs$region_name, "region")
  expect_equal(s3fs$retries, 2)
  expect_equal(s3fs$multipart_threshold, 2 * MB)
  expect_equal(s3fs$s3_client$.internal$config$endpoint[[1]], "endpoint")
  expect_equal(s3fs$s3_client$.internal$config$credentials$creds$access_key_id[[1]], "key")
  expect_equal(s3fs$s3_client$.internal$config$credentials$creds$secret_access_key[[1]], "secret")
  expect_equal(s3fs$s3_client$.internal$config$credentials$creds$session_token[[1]], "token")
  expect_equal(s3fs$s3_client$.internal$config$credentials$profile[[1]], "profile")
})
