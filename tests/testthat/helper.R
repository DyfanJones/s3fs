# helper function to skip test
skip_if_no_env <- function(){
  id = nzchar(Sys.getenv("AWS_ACCESS_KEY_ID"))
  secret = nzchar(Sys.getenv("AWS_SECRET_ACCESS_KEY"))
  region = nzchar(Sys.getenv("AWS_REGION"))
  bucket_nv = nzchar(Sys.getenv("AWS_S3_BUCKET_NOT_VERSIONED"))
  bucket_v = nzchar(Sys.getenv("AWS_S3_BUCKET_VERSIONED"))
  if(!any(id, secret, region, bucket_nv,bucket_v))
    skip("Environment variables are not set for testing")
}
