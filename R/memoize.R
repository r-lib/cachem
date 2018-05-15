#' @export
memoize <- function(f, cache = DiskCache$new()) {
  # f must be forced here to allow code like `g <- memoize(g)` to work.
  # Otherwise it'll try to recurse infinitely on first invocation. Also compile
  # f; otherwise its hash will change on the first two invocations as the
  # compiler keeps count of how many times it has run, and when it compiles it.
  f <- compiler::cmpfun(f)

  structure(
    function(...) {
      # We could force users to pass in a string for the key, and use that
      # directly. However, it's safer in general to hash the key, because
      # otherwise, the key could have odd characters that might not work on
      # the filesystem. Hashing is a simple way to guarantee that we'll have
      # a safe filename.
      args <- list(f, ...)

      key <- digest::digest(args, "sha256")

      if (cache$has(key)) {
        message("Fetching from cache")
        return(cache$get(key))
      }

      value <- f(...)
      cache$set(key, value)

      value
    },
    class = "indexable",
    impl = cache
  )
}


#' @export
`$.indexable` <- function(x, name) {
  attr(x, "impl", exact = TRUE)[[name]]
}
