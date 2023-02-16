library(data.table)

bucket_nv = Sys.getenv("AWS_S3_BUCKET_NOT_VERSIONED")
bucket_v = Sys.getenv("AWS_S3_BUCKET_VERSIONED")

test_that("create file in a non version bucket", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  path = s3_file_temp(tmp_dir = bucket_nv)

  s3_file_create(path)
  result = s3_file_exists(path)
  s3_file_delete(path)

  expect_true(result)
})

test_that("create file in a version bucket", {
  skip_if_no_env()
  s3_file_system(refresh = T)

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
  s3_file_system(refresh = T)

  path = s3_file_temp(tmp_dir = bucket_nv)

  s3_file_create(path)
  new_path = s3_file_copy(path, s3_path(bucket_nv, "file1.txt"))

  result = s3_file_exists(new_path)
  s3_file_delete(c(path, new_path))

  expect_true(result)
})

test_that("copy file local to uri standard", {
  skip_if_no_env()
  s3_file_system(refresh = T)

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
  s3_file_system(refresh = T)

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
  s3_file_system(multipart_threshold = fs_bytes("50MB"), refresh = T)

  new_path = s3_file_temp(tmp_dir = bucket_nv)
  path = s3_path(bucket_nv, "multipart_file.csv")

  s3_file_copy(
    path,
    new_path,
    max_batch = fs_bytes("20MB")
  )
  result = s3_file_exists(new_path)
  size = s3_file_size(c(path, new_path))

  s3_file_delete(new_path)

  expect_true(result)
  expect_equal(size[1], size[2])
})

test_that("copy file local to uri multipart", {
  skip_if_no_env()
  s3_file_system(multipart_threshold = fs_bytes("2MB"), refresh = T)

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
    max_batch = fs_bytes("6MB"),
    overwrite = T
  )

  result = s3_file_exists(new_path)

  expect_true(result)
  expect_equal(unname(fs::file_size(path)), s3_file_size(new_path))

  s3_file_delete(new_path)
  fs::file_delete(path)
})

################################################################################
# Move
################################################################################
test_that("move file", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  path = s3_file_temp(tmp_dir = bucket_nv)
  new_path = s3_file_temp(tmp_dir = bucket_nv)

  s3_file_create(path)

  s3_file_move(path, new_path)
  found = s3_file_exists(c(path, new_path))

  s3_file_delete(new_path)

  expect_false(found[1])
  expect_true(found[2])
})

################################################################################
# tag
################################################################################
test_that("get file tags", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  path = s3_file_temp(tmp_dir = bucket_nv)

  s3_file_create(path)
  tag_info = s3_file_tag_info(path)

  s3_file_delete(path)

  expect_false(nzchar(tag_info$tag_key))
  expect_false(nzchar(tag_info$tag_value))
})

test_that("overwrite file tags", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  path = s3_file_temp(tmp_dir = bucket_nv)

  s3_file_create(path)
  s3_file_tag_update(path, list("tag1" = "value1"), overwrite = T)
  tag_info = s3_file_tag_info(path)

  s3_file_delete(path)

  expect_equal(tag_info$tag_key, "tag1")
  expect_equal(tag_info$tag_value, "value1")
})

test_that("modify existing file tags", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  path = s3_file_temp(tmp_dir = bucket_nv)

  s3_file_create(path)
  s3_file_tag_update(path, list("tag1" = "value1"))
  s3_file_tag_update(path, list("tag2" = "value2"))
  tag_info = s3_file_tag_info(path)

  s3_file_delete(path)

  expect_equal(tag_info$tag_key, c("tag1", "tag2"))
  expect_equal(tag_info$tag_value, c("value1", "value2"))
})

test_that("delete existing file tags", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  path = s3_file_temp(tmp_dir = bucket_nv)

  s3_file_create(path)
  s3_file_tag_update(path, list("tag1" = "value1"))
  s3_file_tag_delete(path)
  tag_info = s3_file_tag_info(path)

  s3_file_delete(path)

  expect_false(nzchar(tag_info$tag_key))
  expect_false(nzchar(tag_info$tag_value))
})

################################################################################
# Stream: Standard
################################################################################

test_that("stream files in and out standard", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  obj = list(charToRaw("contents1"), charToRaw("contents2"))

  dir = s3_file_temp(tmp_dir = bucket_nv)
  path = s3_path(dir, letters[1:2], ext = "txt")

  s3_file_stream_out(obj, path)
  result = lapply(s3_file_stream_in(path), rawToChar)

  s3_file_delete(path)
  expect_equal(result, list("contents1", "contents2"))
})

################################################################################
# Stream: multipart
################################################################################

test_that("stream files in and out multipart", {
  skip_if_no_env()
  s3_file_system(multipart_threshold = fs_bytes("2MB"), refresh = T)

  path = "temp.csv"
  new_path = s3_file_temp(tmp_dir = bucket_nv, ext = "csv")

  size = 1e6
  df = data.frame(
    var1 = sample(LETTERS, size, TRUE),
    var3 = 1:size
  )
  fwrite(df, path)
  obj = readBin(path, "raw", n = file.size(path))

  s3_file_stream_out(obj, new_path, max_batch = fs_bytes("6MB"), overwrite = T)
  result = s3_file_stream_in(new_path)

  s3_file_delete(new_path)
  fs::file_delete(path)
  expect_equal(obj, result[[1]])
})

################################################################################
# Touch
################################################################################
test_that("touch file", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  path = s3_file_temp(tmp_dir = bucket_nv)

  s3_file_create(path)
  time_stamp_1 = s3_file_info(path)$last_modified

  # wait 1 second
  Sys.sleep(1)
  s3_file_touch(path)
  time_stamp_2 = s3_file_info(path)$last_modified

  s3_file_delete(path)

  expect_true(time_stamp_1 < time_stamp_2)
})

################################################################################
# Version
################################################################################
test_that("version id file", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  obj1 = charToRaw("contents1")
  obj2 = charToRaw("contents2")

  path = s3_file_temp(tmp_dir = bucket_v)

  s3_file_stream_out(obj1, path)
  s3_file_stream_out(obj2, path, overwrite = T)

  versions = s3_file_version_info(path)

  result1 = rawToChar(s3_file_stream_in(versions$uri[[2]])[[1]])
  result2 = rawToChar(s3_file_stream_in(versions$uri[[1]])[[1]])

  s3_file_delete(path)
  expect_equal(result1, "contents1")
  expect_equal(result2, "contents2")
})

################################################################################
# Url
################################################################################
test_that("file url", {
  skip_if_no_env()

  url = s3_file_url("s3://madeup/dummy.txt")
  expect_true(
    grepl(
      "https://madeup.s3.*amazonaws.com/dummy.txt\\?AWSAccessKeyId=.*&Expires=.*&Signature=.*",
      url
    )
  )
})
