
test_that("validate_key", {
  expect_true(validate_key("e"))
  expect_true(validate_key("abc"))
  expect_true(validate_key("abcd123-_"))
  expect_true(validate_key("-"))
  expect_true(validate_key("_"))

  expect_error(validate_key("a.b"))
  expect_error(validate_key("a,b"))
  expect_error(validate_key("é"))
  expect_error(validate_key("ABC"))
  expect_error(validate_key("_A"))
  expect_error(validate_key("!"))
  expect_error(validate_key("a b"))
  expect_error(validate_key("ab\n"))
})
