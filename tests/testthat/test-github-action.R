test_that("check if github actions passing environ variables", {
  foo = Sys.getenv("FOO")
  expect_equal(foo, "bar")
})
