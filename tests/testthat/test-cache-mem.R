time_factor <- 1
# Do things slower on GHA because of slow machines
if (is_on_github_actions()) time_factor <- 4


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

test_that("cache_mem: reset", {
  mc <- cache_mem()
  mc$set("a", "A")
  mc$set("b", "B")
  mc$reset()
  expect_identical(mc$keys(), character())
  expect_identical(mc$size(), 0L)
  mc$set("c", "C")
  expect_identical(mc$keys(), "c")
  expect_identical(mc$size(), 1L)
  expect_false(mc$exists("a"))
  expect_true(mc$exists("c"))
})

test_that("cache_mem: pruning respects max_n", {
  delay <- 0.001 * time_factor
  d <- cache_mem(max_n = 3)
  # NOTE: The short delays after each item are meant to tests more reliable on
  # CI systems.
  d$set("a", rnorm(100)); Sys.sleep(delay)
  d$set("b", rnorm(100)); Sys.sleep(delay)
  d$set("c", rnorm(100)); Sys.sleep(delay)
  d$set("d", rnorm(100)); Sys.sleep(delay)
  d$set("e", rnorm(100)); Sys.sleep(delay)
  expect_identical(sort(d$keys()), c("c", "d", "e"))
})

test_that("cache_mem: pruning respects max_size", {
  delay <- 0.001 * time_factor
  d <- cache_mem(max_size = object.size(123) * 3)
  d$set("a", rnorm(100)); Sys.sleep(delay)
  d$set("b", rnorm(100)); Sys.sleep(delay)
  d$set("c", 1);          Sys.sleep(delay)
  expect_identical(sort(d$keys()), c("c"))
  d$set("d", rnorm(100)); Sys.sleep(delay)
  # Objects are pruned with oldest first, so even though "c" would fit in the
  # cache, it is removed after adding "d" (and "d" is removed as well because it
  # doesn't fit).
  expect_length(d$keys(), 0)
  d$set("e", 2);          Sys.sleep(delay)
  d$set("f", 3);          Sys.sleep(delay)
  expect_identical(sort(d$keys()), c("e", "f"))
})

test_that("cache_mem: max_size=Inf", {
  mc <- cachem::cache_mem(max_size = Inf)
  mc$set("a", 123)
  expect_identical(mc$get("a"), 123)
  mc$prune()
  expect_identical(mc$get("a"), 123)
})

test_that("cache_mem: pruning respects both max_n and max_size", {
  delay <- 0.001 * time_factor
  d <- cache_mem(max_n = 3, max_size = object.size(123) * 3)
  # Set some values. Use rnorm so that object size is large; a simple vector
  # like 1:100 will be stored very efficiently by R's ALTREP, and won't exceed
  # the max_size. We want each of these objects to exceed max_size so that
  # they'll be pruned.
  d$set("a", rnorm(100)); Sys.sleep(delay)
  d$set("b", rnorm(100)); Sys.sleep(delay)
  d$set("c", rnorm(100)); Sys.sleep(delay)
  d$set("d", rnorm(100)); Sys.sleep(delay)
  d$set("e", rnorm(100)); Sys.sleep(delay)
  d$set("f", 1);          Sys.sleep(delay)
  d$set("g", 1);          Sys.sleep(delay)
  d$set("h", 1);          Sys.sleep(delay)
  expect_identical(sort(d$keys()), c("f", "g", "h"))

  # This will cause f to be pruned (due to max_n) and g to be pruned (due to
  # max_size).
  d$set("i", c(2, 3));    Sys.sleep(0.001)
  expect_identical(sort(d$keys()), c("h", "i"))
})

test_that('cache_mem: pruning with evict="lru"', {
  delay <- 0.001 * time_factor
  d <- cache_mem(max_n = 2)
  d$set("a", 1); Sys.sleep(delay)
  d$set("b", 1); Sys.sleep(delay)
  d$set("c", 1); Sys.sleep(delay)
  expect_identical(sort(d$keys()), c("b", "c"))
  d$get("b")
  d$set("d", 1); Sys.sleep(delay)
  expect_identical(sort(d$keys()), c("b", "d"))
  d$get("b")
  d$set("e", 2); Sys.sleep(delay)
  d$get("b")
  d$set("f", 3); Sys.sleep(delay)
  expect_identical(sort(d$keys()), c("b", "f"))

  d <- cache_mem(max_n = 2, evict = "lru")
  d$set("a", 1); Sys.sleep(delay)
  d$set("b", 1); Sys.sleep(delay)
  d$set("c", 1); Sys.sleep(delay)
  d$set("b", 2); Sys.sleep(delay)
  d$set("d", 2); Sys.sleep(delay)
  expect_identical(sort(d$keys()), c("b", "d"))
})

test_that('cache_mem: pruning with evict="fifo"', {
  delay <- 0.001 * time_factor
  d <- cache_mem(max_n = 2, evict = "fifo")
  d$set("a", 1); Sys.sleep(delay)
  d$set("b", 1); Sys.sleep(delay)
  d$set("c", 1); Sys.sleep(delay)
  expect_identical(sort(d$keys()), c("b", "c"))
  d$get("b")
  d$set("d", 1); Sys.sleep(delay)
  expect_identical(sort(d$keys()), c("c", "d"))
  d$get("b")
  d$set("e", 2); Sys.sleep(delay)
  d$get("b")
  d$set("f", 3); Sys.sleep(delay)
  expect_identical(sort(d$keys()), c("e", "f"))

  d <- cache_mem(max_n = 2, evict = "fifo")
  d$set("a", 1); Sys.sleep(delay)
  d$set("b", 1); Sys.sleep(delay)
  d$set("c", 1); Sys.sleep(delay)
  d$set("b", 2); Sys.sleep(delay)
  d$set("d", 2); Sys.sleep(delay)
  expect_identical(sort(d$keys()), c("b", "d"))
})

test_that("Pruning by max_age", {
  skip_on_cran()

  # Should prune target item on get()
  d <- cache_mem(max_age = 0.25*time_factor)
  d$set("a", 1)
  expect_identical(d$get("a"), 1)
  Sys.sleep(0.3*time_factor)
  expect_identical(d$get("a"), key_missing())
  expect_identical(d$get("x"), key_missing())

  # Should prune all items on set()
  d <- cache_mem(max_age = 0.25*time_factor)
  d$set("a", 1)
  expect_identical(d$get("a"), 1)
  Sys.sleep(0.3*time_factor)
  d$set("b", 1)
  expect_identical(d$keys(), "b")

  # Should prune target item on exists()
  d <- cache_mem(max_age = 0.25*time_factor)
  d$set("a", 1)
  expect_identical(d$get("a"), 1)
  expect_true(d$exists("a"))
  expect_false(d$exists("b"))
  Sys.sleep(0.15*time_factor)
  d$set("b", 1)
  expect_true(d$exists("a"))
  expect_true(d$exists("b"))
  Sys.sleep(0.15*time_factor)
  expect_false(d$exists("a"))
  expect_true(d$exists("b"))

  # Should prune all items on keys()
  d <- cache_mem(max_age = 0.25*time_factor)
  d$set("a", 1)
  expect_identical(d$keys(), "a")
  Sys.sleep(0.15*time_factor)
  d$set("b", 1)
  Sys.sleep(0.15*time_factor)
  expect_identical(d$keys(), "b")

  # Should prune all items on size()
  d <- cache_mem(max_age = 0.25*time_factor)
  d$set("a", 1)
  expect_identical(d$size(), 1L)
  Sys.sleep(0.15*time_factor)
  d$set("b", 1)
  expect_identical(d$size(), 2L)
  Sys.sleep(0.15*time_factor)
  expect_identical(d$size(), 1L)
})

test_that("TTL", {
  skip_on_cran()

  # positive ttl
  d <- cache_mem(max_age = 0.25*time_factor)
  d$set("a", 1)
  expect_identical(d$size(), 1L)
  Sys.sleep(0.15*time_factor)
  expect_true(d$ttl("a") > 0)

  # after expiration, converts to NA
  d <- cache_mem(max_age = 0.25*time_factor)
  d$set("a", 1)
  expect_identical(d$size(), 1L)
  Sys.sleep(0.3*time_factor)
  expect_identical(d$ttl("a"), NA_real_)

  # no-limit returns Inf
  d <- cache_mem()
  d$set("a", 1)
  expect_identical(d$size(), 1L)
  Sys.sleep(0.15*time_factor)
  expect_identical(d$ttl("a"), Inf)
})

test_that("Removed objects can be GC'd", {
  mc <- cache_mem()
  e <- new.env()
  finalized <- FALSE
  reg.finalizer(e, function(x) finalized <<- TRUE)
  mc$set("e", e)
  rm(e)
  mc$set("x", 1)
  gc()
  expect_false(finalized)
  expect_true(is.environment(mc$get("e")))
})

test_that("Pruned objects can be GC'd", {
  delay <- 0.001 * time_factor
  # Cache is large enough to hold one environment and one number
  mc <- cache_mem(max_size = object.size(new.env()) + object.size(1234))
  e <- new.env()
  finalized <- FALSE
  reg.finalizer(e, function(x) finalized <<- TRUE)
  mc$set("e", e)
  rm(e)
  mc$set("x", 1)
  gc()
  expect_false(finalized)
  expect_true(is.environment(mc$get("e")))

  # Get x so that the atime is updated
  Sys.sleep(delay)
  mc$get("x")
  Sys.sleep(delay)

  # e should be pruned when we add another item
  mc$set("y", 2)
  gc()
  expect_true(finalized)
  expect_true(is.key_missing(mc$get("e")))
})


# For https://github.com/r-lib/cachem/issues/47, https://github.com/r-lib/cachem/pull/48/
test_that("Cache doesn't shrink smaller than INITIAL_SIZE", {
  # This test also makes sure that the cache doesn't keep adding elements to the
  # storage vectors when there are zero items, then an item is added and
  # removed, repeatedly.
  m <- cache_mem()
  e <- environment(m$get)
  for (i in seq_len(e$INITIAL_SIZE)) {
    m$set(as.character(i), i)
    m$remove(as.character(i))
  }
  expect_equal(e$total_n_, 0)
  expect_equal(e$last_idx_, e$INITIAL_SIZE)
  expect_length(e$key_, e$INITIAL_SIZE)
  expect_length(e$value_, e$INITIAL_SIZE)

  # Adding one more item should trigger a compact_()
  m$set("a", 1)
  m$remove("a")

  expect_equal(e$total_n_, 0)
  # last_idx_ should be reset after we pass the INITIAL_SIZE, even if there are
  # no items in the cache. Prior to the fix in #48, it could keep growing.
  expect_equal(e$last_idx_, 0)
  expect_length(e$key_, e$INITIAL_SIZE)
  expect_length(e$value_, e$INITIAL_SIZE)
})
