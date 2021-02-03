test_that("cache_mem: handling missing values", {
  d <- cache_mem()
  expect_true(is.key_missing(d$get("abcd")))
  d$set("a", 100)
  expect_identical(d$get("a"), 100)
  expect_identical(d$get("y", missing = NULL), NULL)
  expect_error(
    d$get("y", missing = stop("Missing key")),
    "^Missing key$",
  )

  d <- cache_mem(missing = NULL)
  expect_true(is.null(d$get("abcd")))
  d$set("a", 100)
  expect_identical(d$get("a"), 100)
  expect_identical(d$get("y", missing = -1), -1)
  expect_error(
    d$get("y", missing = stop("Missing key")),
    "^Missing key$",
  )

  d <- cache_mem(missing = stop("Missing key"))
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
  d <- cache_mem(missing = !!expr)
  expect_error(d$get("y"), "^Missing key$")
  expect_error(d$get("y"), "^Missing key$") # Make sure a second time also throws
})



test_that("cache_mem: pruning respects max_n", {
  d <- cache_mem(max_n = 3)
  d$set("a", rnorm(100))
  d$set("b", rnorm(100))
  d$set("c", rnorm(100))
  d$set("d", rnorm(100))
  d$set("e", rnorm(100))
  expect_setequal(d$keys(), c("c", "d", "e"))
})

test_that("cache_mem: pruning respects max_size", {
  d <- cache_mem(max_size = 200)
  d$set("a", rnorm(100))
  d$set("b", rnorm(100))
  d$set("c", 1)
  expect_setequal(d$keys(), c("c"))
  d$set("d", rnorm(100))
  # Objects are pruned with oldest first, so even though "c" would fit in the
  # cache, it is removed after adding "d" (and "d" is removed as well because it
  # doesn't fit).
  expect_length(d$keys(), 0)
  d$set("e", 2)
  d$set("f", 3)
  expect_setequal(d$keys(), c("e", "f"))
})

test_that("cache_mem: pruning respects both max_n and max_size", {
  d <- cache_mem(max_n = 3, max_size = 200)
  # Set some values. Use rnorm so that object size is large; a simple vector
  # like 1:100 will be stored very efficiently by R's ALTREP, and won't exceed
  # the max_size. We want each of these objects to exceed max_size so that
  # they'll be pruned.
  d$set("a", rnorm(100))
  d$set("b", rnorm(100))
  d$set("c", rnorm(100))
  d$set("d", rnorm(100))
  d$set("e", rnorm(100))
  d$set("f", 1)   # This object is small and shouldn't be pruned.
  d$set("g", 1)
  d$set("h", 1)
  d$set("i", 1)
  expect_setequal(d$keys(), c("g", "h", "i"))
})
