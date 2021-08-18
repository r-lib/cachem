time_factor <- 1
# Do things slower on GHA because of slow machines
if (is_on_github_actions()) time_factor <- 4


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


test_that("cache_disk: pruning respects max_n", {
  # Timing is apparently unreliable on CRAN, so skip tests there. It's possible
  # that a heavily loaded system will have issues with these tests because of
  # the time resolution.
  skip_on_cran()
  delay <- 0.01 * time_factor
  d <- cache_disk(max_n = 3)
  # NOTE: The short delays after each item are meant to tests more reliable on
  # CI systems.
  d$set("a", rnorm(100)); Sys.sleep(delay)
  d$set("b", rnorm(100)); Sys.sleep(delay)
  d$set("c", rnorm(100)); Sys.sleep(delay)
  d$set("d", rnorm(100)); Sys.sleep(delay)
  d$set("e", rnorm(100)); Sys.sleep(delay)
  d$prune()
  expect_identical(sort(d$keys()), c("c", "d", "e"))
})

test_that("cache_disk: pruning respects max_size", {
  skip_on_cran()
  delay <- 0.01 * time_factor
  d <- cache_disk(max_size = 200)
  d$set("a", rnorm(100)); Sys.sleep(delay)
  d$set("b", rnorm(100)); Sys.sleep(delay)
  d$set("c", 1);          Sys.sleep(delay)
  d$prune()
  expect_identical(sort(d$keys()), c("c"))
  d$set("d", rnorm(100)); Sys.sleep(delay)
  # Objects are pruned with oldest first, so even though "c" would fit in the
  # cache, it is removed after adding "d" (and "d" is removed as well because it
  # doesn't fit).
  d$prune()
  expect_length(d$keys(), 0)
  d$set("e", 2);          Sys.sleep(delay)
  d$set("f", 3);          Sys.sleep(delay)
  d$prune()
  expect_identical(sort(d$keys()), c("e", "f"))
})

# Issue shiny#3033
test_that("cache_disk: pruning respects both max_n and max_size", {
  skip_on_cran()
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
  Sys.sleep(0.1*time_factor)  # For systems that have low mtime resolution.
  d$set("f", 1)   # This object is small and shouldn't be pruned.
  d$prune()
  expect_identical(d$keys(), "f")
})

# Return TRUE if the Sys.setFileTime() has subsecond resolution, FALSE
# otherwise.
setfiletime_has_subsecond_resolution <- function() {
  tmp <- tempfile()
  file.create(tmp)
  Sys.setFileTime(tmp, Sys.time())
  time <- as.numeric(file.info(tmp)[['mtime']])
  if (time == floor(time)) {
    return(FALSE)
  } else {
    return(TRUE)
  }
}

test_that('cache_disk: pruning with evict="lru"', {
  skip_on_cran()
  delay <- 0.01 * time_factor
  # For lru tests, make sure there's sub-second resolution for
  # Sys.setFileTime(), because that's what the lru code uses to update times.
  skip_if_not(
    setfiletime_has_subsecond_resolution(),
    "Sys.setFileTime() does not have subsecond resolution on this platform."
  )

  d <- cache_disk(max_n = 2)
  d$set("a", 1); Sys.sleep(delay)
  d$set("b", 1); Sys.sleep(delay)
  d$set("c", 1); Sys.sleep(delay)
  d$prune()
  expect_identical(sort(d$keys()), c("b", "c"))
  d$get("b");    Sys.sleep(delay)
  d$set("d", 1); Sys.sleep(delay)
  d$prune()
  expect_identical(sort(d$keys()), c("b", "d"))
  d$get("b");    Sys.sleep(delay)
  d$set("e", 2); Sys.sleep(delay)
  d$get("b");    Sys.sleep(delay)
  d$set("f", 3); Sys.sleep(delay)
  d$prune()
  expect_identical(sort(d$keys()), c("b", "f"))
})

test_that('cache_disk: pruning with evict="fifo"', {
  skip_on_cran()
  delay <- 0.01 * time_factor
  d <- cache_disk(max_n = 2, evict = "fifo")
  d$set("a", 1); Sys.sleep(delay)
  d$set("b", 1); Sys.sleep(delay)
  d$set("c", 1); Sys.sleep(delay)
  d$prune()
  expect_identical(sort(d$keys()), c("b", "c"))
  d$get("b")
  d$set("d", 1); Sys.sleep(delay)
  d$prune()
  expect_identical(sort(d$keys()), c("c", "d"))
  d$get("b")
  d$set("e", 2); Sys.sleep(delay)
  d$get("b")
  d$set("f", 3); Sys.sleep(delay)
  d$prune()
  expect_identical(sort(d$keys()), c("e", "f"))
})


test_that("cache_disk: pruning throttling", {
  skip_on_cran()
  delay <- 0.01 * time_factor

  # Pruning won't happen when the number of items is less than prune_rate AND
  # the set() calls happen within 5 seconds.
  d <- cache_disk(max_n = 2, prune_rate = 20)
  d$set("a", 1); Sys.sleep(delay)
  d$set("b", 1); Sys.sleep(delay)
  d$set("c", 1); Sys.sleep(delay)
  d$set("d", 1); Sys.sleep(delay)
  expect_identical(sort(d$keys()), c("a", "b", "c", "d"))

  # Pruning will happen with a lower prune_rate value.
  d <- cache_disk(max_n = 2, prune_rate = 3)
  # Normally the throttle counter starts with a random value, but for these
  # tests we need to make it deterministic.
  environment(d$set)$prune_throttle_counter_ <- 0
  d$set("a", 1); Sys.sleep(delay)
  d$set("b", 1); Sys.sleep(delay)
  d$set("c", 1); Sys.sleep(delay)
  expect_identical(sort(d$keys()), c("b", "c"))
  d$set("d", 1); Sys.sleep(delay)
  expect_identical(sort(d$keys()), c("b", "c", "d"))
  d$set("e", 1); Sys.sleep(delay)
  expect_identical(sort(d$keys()), c("b", "c", "d", "e"))
  d$set("f", 1); Sys.sleep(delay)
  expect_identical(sort(d$keys()), c("e", "f"))

  # After a 5 second delay, on the next set(), pruning will not be throttled.
  Sys.sleep(5)
  d$set("f", 1); Sys.sleep(delay)
  expect_identical(sort(d$keys()), c("e", "f"))
})

test_that("destroy_on_finalize works", {
  d <- cache_disk(destroy_on_finalize = TRUE)
  cache_dir <- d$info()$dir

  expect_true(dir.exists(cache_dir))
  rm(d)
  gc()
  expect_false(dir.exists(cache_dir))
})


test_that("Warnings for caching reference objects", {
  d <- cache_disk(warn_ref_objects = TRUE)
  expect_warning(d$set("a", new.env()))
  expect_warning(d$set("a", function() NULL))
  expect_warning(d$set("a", fastmap()))  # fastmap objects contain an external pointer

  # Default is to not warn on ref objects
  d <- cache_disk()
  expect_silent(d$set("a", new.env()))
  expect_silent(d$set("a", function() NULL))
  expect_silent(d$set("a", fastmap()))
})
