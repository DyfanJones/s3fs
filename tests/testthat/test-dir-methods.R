bucket_nv = Sys.getenv("AWS_S3_BUCKET_NOT_VERSIONED")

################################################################################
# Copy: standard
################################################################################

test_that("copy file uri to uri standard", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  dir = s3_file_temp(tmp_dir = bucket_nv)
  s3_dir_create(dir)

  path = s3_path(dir, "file", letters[1:3], ext = "txt")
  s3_file_create(path)
  path = s3_path(dir, letters[1:3], ext = "txt")
  s3_file_create(path)
  new_path = s3_file_temp(tmp_dir = bucket_nv)

  s3_dir_copy(dir, new_path)

  result = s3_file_exists(s3_dir_ls(new_path, recurse = T))
  file_path = path_rel(s3_dir_ls(dir, recurse = T), dir)
  result_file_path = path_rel(s3_dir_ls(new_path, recurse = T), new_path)

  s3_dir_delete(c(dir, new_path))

  expect_true(all(result))
  expect_equal(file_path, result_file_path)
})

test_that("copy file local to uri standard", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  dir = fs::file_temp()
  fs::dir_create(fs::path(dir, "file"))
  new_path = fs::path(dir, "file", letters[1:3], ext = "txt")
  fs::file_create(new_path)
  new_path = fs::path(dir, letters[1:3], ext = "txt")
  fs::file_create(new_path)

  new_path = s3_file_temp(tmp_dir = bucket_nv)

  s3_dir_copy(dir, new_path)

  result = s3_file_exists(s3_dir_ls(new_path, recurse = T))

  result_files = path_rel(s3_dir_ls(new_path, recurse = T), new_path)
  expect_files =as.character(path_rel(fs::dir_ls(dir, type = "file", recurse = T), dir))
  s3_dir_delete(new_path)
  fs::dir_delete(dir)

  expect_true(all(result))
  expect_equal(result_files, expect_files)
})

test_that("copy file uri to local standard", {
  skip_if_no_env()
  s3_file_system(refresh = T)

  dir = s3_file_temp(tmp_dir = bucket_nv)
  path = s3_path(dir, "file", letters[1:3], ext = "txt")
  s3_file_create(path)
  path = s3_path(dir, letters[1:3], ext = "txt")
  s3_file_create(path)
  new_path = fs::file_temp()

  s3_dir_copy(dir, new_path)

  file_path = fs::dir_ls(new_path, recurse = T, type = "file")
  path = s3_dir_ls(dir, recurse = T)


  result = fs::file_exists(file_path)
  s3_dir_delete(dir)
  fs::dir_delete(new_path)

  expect_true(all(result))
  expect_equal(as.character(path_rel(file_path, new_path)), path_rel(path, dir))
})
