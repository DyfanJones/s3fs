test_that("check if retry is working correctly", {
  logger = lgr::get_logger("s3fs")

  err_fun = function() stop(
    structure(list(message = "error"), class = c("http_500", "error", "condition"))
  )
  expect_error(
    retry_api_call(
      err_fun(),
      2
    )
  )
  expect_true(grepl("0.4", logger$last_event$msg))
})
