library(data.table)

bucket_nv = Sys.getenv("AWS_S3_BUCKET_NOT_VERSIONED")
bucket_v = Sys.getenv("AWS_S3_BUCKET_VERSIONED")

test_that("create file in a non version bucket", {
  skip_if_no_env()
  path = s3_file_temp(tmp_dir = bucket_nv)

  s3_file_create(path)
  result = s3_file_exists(path)
  s3_file_delete(path)

  expect_true(result)
})

test_that("create file in a version bucket", {
  skip_if_no_env()
  path = s3_file_temp(tmp_dir = bucket_v)

  s3_file_create(path)
  result = s3_file_exists(path)
  s3_file_delete(path)

  expect_true(result)
})

################################################################################
# Copy: Standard
################################################################################

test_that("copy file uri to uri standard", {
  skip_if_no_env()
  path = s3_file_temp(tmp_dir = bucket_nv)

  s3_file_create(path)
  new_path = s3_file_copy(path, s3_path(bucket_nv, "file1.txt"))

  result = s3_file_exists(new_path)
  s3_file_delete(c(path, new_path))

  expect_true(result)
})

test_that("copy file local to uri standard", {
  skip_if_no_env()
  path = fs::file_temp()
  fs::file_create(path)

  new_path = s3_file_copy(path, s3_path(bucket_nv, "file1.txt"))

  result = s3_file_exists(new_path)
  s3_file_delete(new_path)
  fs::file_delete(path)

  expect_true(result)
})

test_that("copy file uri to local standard", {
  skip_if_no_env()
  path = s3_file_temp(tmp_dir = bucket_nv)
  new_path = fs::file_temp()

  s3_file_create(path)
  s3_file_copy(path, new_path)

  result = fs::file_exists(new_path)
  s3_file_delete(path)
  fs::file_delete(new_path)

  expect_true(result)
})

################################################################################
# Copr: Multipart
################################################################################

test_that("copy file uri to uri multipart", {
  skip_if_no_env()
  s3_file_system(multipart_threshold = 2 * s3fs:::MB, refresh = T)

  new_path = s3_file_temp(tmp_dir = bucket_nv)
  path = s3_path(bucket_nv, "multipart_file.csv")

  s3_file_copy(
    path,
    new_path,
    max_batch = 6 * s3fs:::MB
  )
  result = s3_file_exists(new_path)
  size = s3_file_size(c(path, new_path))

  s3_file_delete(new_path)

  expect_true(result)
  expect_equal(size[1], size[2])
})

test_that("copy file local to uri multipart", {
  skip_if_no_env()
  s3_file_system(multipart_threshold = 2 * s3fs:::MB, refresh = T)

  path = "temp.csv"
  new_path = s3_file_temp(tmp_dir = bucket_nv, ext = "csv")

  size = 1e6
  df = data.frame(
    var1 = sample(LETTERS, size, TRUE),
    var3 = 1:size
  )
  fwrite(df, path)

  s3_file_copy(
    path,
    new_path,
    max_batch = 6 * s3fs:::MB,
    overwrite = T
  )

  result = s3_file_exists(new_path)

  expect_true(result)
  expect_equal(file.size(path), s3_file_size(new_path))

  s3_file_delete(new_path)
  fs::file_delete(path)
})

################################################################################
# Copr: Multipart
################################################################################

test_that("stream in files in and out", {
  skip_if_no_env()

  bucket = s3_bucket_create(s3_file_temp("bucket"))

  obj = list(charToRaw("contents1"), charToRaw("contents2"))

  dir = s3_file_temp(tmp_dir = bucket)
  path = s3_path(dir, letters[1:2], ext = "txt")

  s3_file_stream_out(obj, path)
  result = lapply(s3_file_stream_in(path), rawToChar)

  s3_bucket_delete(bucket)
  expect_equal(result, list("contents1", "contents2"))
})

