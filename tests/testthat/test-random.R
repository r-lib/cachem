test_that("cache objects don't affect RNG state", {
  # https://github.com/r-lib/cachem/issues/29
  cm <- cache_mem()
  cd <- cache_disk()

  set.seed(42)
  target <- sample(99999, 1)

  set.seed(42)
  cm$set("x",letters)
  expect_identical(sample(99999, 1), target)

  set.seed(42)
  cd$set("x",letters)
  expect_identical(sample(99999, 1), target)
})
