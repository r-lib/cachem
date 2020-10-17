#' Compose any number of cache objects into a new, layered cache object
#'
#' @param ... Cache objects to compose into a new, layered cache object.
#'
#' @export
cache_compose <- function(...) {
  caches <- list(...)

  structure(list(
    get = function(key) {
      message("key: ", key)
      value <- NULL
      # Search down the caches for the object
      for (i in seq_along(caches)) {
        message("Get from ", class(caches[[i]])[1], "... ", appendLF = FALSE)
        value <- caches[[i]]$get(key)

        if (!is.key_missing(value)) {
          # Hit
          message("hit")
          # Set the value in any caches where we searched and missed.
          for (j in seq_len(i-1)) {
            caches[[j]]$set(key, value)
          }
          break
        } else {
          message("miss")
        }
      }

      value
    },

    set = function(key, value) {
      for (cache in caches) {
        cache$set(key, value)
      }
    },

    exists = function(key) {
      for (cache in caches) {
        if (cache$exists(key)) {
          return(TRUE)
        }
      }
      FALSE
    },

    keys = function() {
      unique(unlist(lapply(caches, function (cache) {
        cache$keys()
      })))
    },

    remove = function(key) {
      for (cache in caches) {
        cache$remove(key)
      }
    },

    reset = function() {
      for (cache in caches) {
        cache$reset()
      }
    },

    get_caches = function() {
      caches
    }
  ), class = "ComposedCache")
}

