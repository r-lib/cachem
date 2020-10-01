test_that("MemoryCache: handling missing values", {
  d <- memoryCache()
  expect_true(is.key_missing(d$get("abcd")))
  d$set("a", 100)
  expect_identical(d$get("a"), 100)
  expect_identical(d$get("y", missing = NULL), NULL)
  expect_error(
    d$get("y", missing = function(key) stop("Missing key: ", key), exec_missing = TRUE),
    "^Missing key: y$",
  )

  d <- memoryCache(missing = NULL)
  expect_true(is.null(d$get("abcd")))
  d$set("a", 100)
  expect_identical(d$get("a"), 100)
  expect_identical(d$get("y", missing = -1), -1)
  expect_error(
    d$get("y", missing = function(key) stop("Missing key: ", key), exec_missing = TRUE),
    "^Missing key: y$",
  )

  d <- memoryCache(missing = function(key) stop("Missing key: ", key), exec_missing = TRUE)
  expect_error(d$get("abcd"), "^Missing key: abcd$")
  # When exec_missing==TRUE, should be able to set a value that's identical to
  # missing.
  d$set("x", NULL)
  d$set("x", function(key) stop("Missing key: ", key))
  d$set("a", 100)
  expect_identical(d$get("a"), 100)
  expect_identical(d$get("y", missing = NULL, exec_missing = FALSE), NULL)
  expect_true(is.key_missing(d$get("y", missing = key_missing(), exec_missing = FALSE)))
  expect_error(
    d$get("y", missing = function(key) stop("Missing key 2: ", key), exec_missing = TRUE),
    "^Missing key 2: y$",
  )

  # Can't create a cache with both missing and missing_f
  expect_error(memoryCache(missing = 1, exec_missing = TRUE))
})
