
test_that("cache_disk: handling missing values", {
  d <- cache_disk()
  expect_true(is.key_missing(d$get("abcd")))
  d$set("a", 100)
  expect_identical(d$get("a"), 100)
  expect_identical(d$get("y", missing = NULL), NULL)
  expect_error(
    d$get("y", missing = stop("Missing key")),
    "^Missing key$",
  )

  d <- cache_disk(missing = NULL)
  expect_true(is.null(d$get("abcd")))
  d$set("a", 100)
  expect_identical(d$get("a"), 100)
  expect_identical(d$get("y", missing = -1), -1)
  expect_error(
    d$get("y", missing = stop("Missing key")),
    "^Missing key$",
  )

  d <- cache_disk(missing = stop("Missing key"))
  expect_error(d$get("abcd"), "^Missing key$")
  d$set("x", NULL)
  d$set("a", 100)
  expect_identical(d$get("a"), 100)
  expect_error(d$get("y"), "^Missing key$")
  expect_identical(d$get("y", missing = NULL), NULL)
  expect_true(is.key_missing(d$get("y", missing = key_missing())))
  expect_error(
    d$get("y", missing = stop("Missing key 2")),
    "^Missing key 2$",
  )

  # Pass in a quosure
  expr <- rlang::quo(stop("Missing key"))
  d <- cache_disk(missing = !!expr)
  expect_error(d$get("y"), "^Missing key$")
  expect_error(d$get("y"), "^Missing key$") # Make sure a second time also throws
})

# Issue shiny#3033
test_that("cache_disk: pruning respects both max_n and max_size", {
  d <- cache_disk(max_n = 3, max_size = 200)
  # Set some values. Use rnorm so that object size is large; a simple vector
  # like 1:100 will be stored very efficiently by R's ALTREP, and won't exceed
  # the max_size. We want each of these objects to exceed max_size so that
  # they'll be pruned.
  d$set("a", rnorm(100))
  d$set("b", rnorm(100))
  d$set("c", rnorm(100))
  d$set("d", rnorm(100))
  d$set("e", rnorm(100))
  Sys.sleep(0.1)  # For systems that have low mtime resolution.
  d$set("f", 1)   # This object is small and shouldn't be pruned.
  d$prune()
  expect_identical(d$keys(), "f")
})


test_that("destroy_on_finalize works", {
  d <- cache_disk(destroy_on_finalize = TRUE)
  cache_dir <- d$info()$dir

  expect_true(dir.exists(cache_dir))
  rm(d)
  gc()
  expect_false(dir.exists(cache_dir))
})
