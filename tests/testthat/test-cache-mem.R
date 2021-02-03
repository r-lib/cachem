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
  # NOTE: The short delays after each item are meant to tests more reliable on
  # CI systems.
  d$set("a", rnorm(100)); Sys.sleep(0.001)
  d$set("b", rnorm(100)); Sys.sleep(0.001)
  d$set("c", rnorm(100)); Sys.sleep(0.001)
  d$set("d", rnorm(100)); Sys.sleep(0.001)
  d$set("e", rnorm(100)); Sys.sleep(0.001)
  expect_identical(sort(d$keys()), c("c", "d", "e"))
})

test_that("cache_mem: pruning respects max_size", {
  d <- cache_mem(max_size = 200)
  d$set("a", rnorm(100)); Sys.sleep(0.001)
  d$set("b", rnorm(100)); Sys.sleep(0.001)
  d$set("c", 1);          Sys.sleep(0.001)
  expect_identical(sort(d$keys()), c("c"))
  d$set("d", rnorm(100)); Sys.sleep(0.001)
  # Objects are pruned with oldest first, so even though "c" would fit in the
  # cache, it is removed after adding "d" (and "d" is removed as well because it
  # doesn't fit).
  expect_length(d$keys(), 0)
  d$set("e", 2);          Sys.sleep(0.001)
  d$set("f", 3);          Sys.sleep(0.001)
  expect_identical(sort(d$keys()), c("e", "f"))
})

test_that("cache_mem: pruning respects both max_n and max_size", {
  d <- cache_mem(max_n = 3, max_size = 200)
  # Set some values. Use rnorm so that object size is large; a simple vector
  # like 1:100 will be stored very efficiently by R's ALTREP, and won't exceed
  # the max_size. We want each of these objects to exceed max_size so that
  # they'll be pruned.
  d$set("a", rnorm(100)); Sys.sleep(0.001)
  d$set("b", rnorm(100)); Sys.sleep(0.001)
  d$set("c", rnorm(100)); Sys.sleep(0.001)
  d$set("d", rnorm(100)); Sys.sleep(0.001)
  d$set("e", rnorm(100)); Sys.sleep(0.001)
  d$set("f", 1);          Sys.sleep(0.001)
  d$set("g", 1);          Sys.sleep(0.001)
  d$set("h", 1);          Sys.sleep(0.001)
  d$set("i", 1);          Sys.sleep(0.001)
  expect_identical(sort(d$keys()), c("g", "h", "i"))
})

test_that('cache_mem: pruning with evict="lru"', {
  d <- cache_mem(max_n = 2)
  d$set("a", 1); Sys.sleep(0.001)
  d$set("b", 1); Sys.sleep(0.001)
  d$set("c", 1); Sys.sleep(0.001)
  expect_identical(sort(d$keys()), c("b", "c"))
  d$get("b")
  d$set("d", 1); Sys.sleep(0.001)
  expect_identical(sort(d$keys()), c("b", "d"))
  d$get("b")
  d$set("e", 2); Sys.sleep(0.001)
  d$get("b")
  d$set("f", 3); Sys.sleep(0.001)
  expect_identical(sort(d$keys()), c("b", "f"))
})

test_that('cache_mem: pruning with evict="fifo"', {
  d <- cache_mem(max_n = 2, evict = "fifo")
  d$set("a", 1); Sys.sleep(0.001)
  d$set("b", 1); Sys.sleep(0.001)
  d$set("c", 1); Sys.sleep(0.001)
  expect_identical(sort(d$keys()), c("b", "c"))
  d$get("b")
  d$set("d", 1); Sys.sleep(0.001)
  expect_identical(sort(d$keys()), c("c", "d"))
  d$get("b")
  d$set("e", 2); Sys.sleep(0.001)
  d$get("b")
  d$set("f", 3); Sys.sleep(0.001)
  expect_identical(sort(d$keys()), c("e", "f"))
})
