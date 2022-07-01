test_that("create s3 uri path", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  path1 = s3_path("hi", "bye", letters[1:2])
  path2 = s3_path("hi", "bye", letters[1:2], ext = "txt")

  expect_equal(path1, c("s3://hi/bye/a", "s3://hi/bye/b"))
  expect_equal(path2, c("s3://hi/bye/a.txt", "s3://hi/bye/b.txt"))
})

test_that("get s3 uri directory", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  path1 = s3_path_dir(c("my_bucket", "s3://my_bucket"))

  path2 = s3_path_dir(c("my_bucket/dir/file.txt", "my_bucket/dir/file.txt"))

  expect_equal(path1, c("s3://my_bucket", "s3://my_bucket"))
  expect_equal(path2, c("s3://my_bucket/dir", "s3://my_bucket/dir"))
})

test_that("get s3 uri file", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  path = s3_path_file(c("my_bucket/dir/file.txt", "s3://my_bucket/dir/file.txt"))

  expect_equal(path, c("file.txt", "file.txt"))
})

test_that("get s3 uri file extension", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  path = s3_path_ext(c("my_bucket/dir/file.txt", "s3://my_bucket/dir/file.txt"))

  expect_equal(path, c("txt", "txt"))
})

test_that("remove s3 uri file extension", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  path = s3_path_ext_remove(c("my_bucket/dir/file.txt", "s3://my_bucket/dir/file.txt"))

  expect_equal(path, c("s3://my_bucket/dir/file", "s3://my_bucket/dir/file"))
})

test_that("replace s3 uri file extension", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  path = s3_path_ext_set(c("my_bucket/dir/file.txt", "s3://my_bucket/dir/file.txt"), "csv")

  expect_equal(path, c("s3://my_bucket/dir/file.csv", "s3://my_bucket/dir/file.csv"))
})

test_that("join s3 uri path", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  path = s3_path_join(c("my_bucket", "dir", "file.txt"))

  expect_equal(path, "s3://my_bucket/dir/file.txt")
})

test_that("split s3 uri path", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  s3_parts1 = s3_path_split(c("s3://my_bucket/dir/file.txt"))
  s3_parts2 = s3_path_split(c("s3://my_bucket/dir/file.txt?versionId=1"))

  expect_equal(s3_parts1, list(list(Bucket="my_bucket", Key = "dir/file.txt", VersionId = NULL)))
  expect_equal(s3_parts2, list(list(Bucket="my_bucket", Key = "dir/file.txt", VersionId = "1")))
})
