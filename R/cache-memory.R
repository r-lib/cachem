#' Create a memory cache object
#'
#' A memory cache object is a key-value store that saves the values in an
#' environment. Objects can be stored and retrieved using the `get()` and
#' `set()` methods. Objects are automatically pruned from the cache according to
#' the parameters `max_size`, `max_age`, `max_n`, and `evict`.
#'
#' In a `cache_mem`, R objects are stored directly in the cache; they are not
#' *not* serialized before being stored in the cache. This contrasts with other
#' cache types, like [cache_disk()], where objects are serialized, and the
#' serialized object is cached. This can result in some differences of behavior.
#' For example, as long as an object is stored in a cache_mem, it will not be
#' garbage collected.
#'
#' @section Missing keys:
#'
#'   The `missing` parameter controls what happens when `get()` is called with a
#'   key that is not in the cache (a cache miss). The default behavior is to
#'   return a [key_missing()] object. This is a *sentinel value* that indicates
#'   that the key was not present in the cache. You can test if the returned
#'   value represents a missing key by using the [is.key_missing()] function.
#'   You can also have `get()` return a different sentinel value, like `NULL`.
#'   If you want to throw an error on a cache miss, you can do so by providing
#'   an expression for `missing`, as in `missing = stop("Missing key")`.
#'
#'   When the cache is created, you can supply a value for `missing`, which sets
#'   the default value to be returned for missing values. It can also be
#'   overridden when `get()` is called, by supplying a `missing` argument. For
#'   example, if you use `cache$get("mykey", missing = NULL)`, it will return
#'   `NULL` if the key is not in the cache.
#'
#'   The `missing` parameter is actually an expression which is evaluated each
#'   time there is a cache miss. A quosure (from the rlang package) can be used.
#'
#'   If you use this, the code that calls `get()` should be wrapped with
#'   [tryCatch()] to gracefully handle missing keys.
#'
#'
#'   @section Cache pruning:
#'
#'   Cache pruning occurs when `set()` is called, or it can be invoked manually
#'   by calling `prune()`.
#'
#'   When a pruning occurs, if there are any objects that are older than
#'   `max_age`, they will be removed.
#'
#'   The `max_size` and `max_n` parameters are applied to the cache as a whole,
#'   in contrast to `max_age`, which is applied to each object individually.
#'
#'   If the number of objects in the cache exceeds `max_n`, then objects will be
#'   removed from the cache according to the eviction policy, which is set with
#'   the `evict` parameter. Objects will be removed so that the number of items
#'   is `max_n`.
#'
#'   If the size of the objects in the cache exceeds `max_size`, then objects
#'   will be removed from the cache. Objects will be removed from the cache so
#'   that the total size remains under `max_size`. Note that the size is
#'   calculated using the size of the files, not the size of disk space used by
#'   the files --- these two values can differ because of files are stored in
#'   blocks on disk. For example, if the block size is 4096 bytes, then a file
#'   that is one byte in size will take 4096 bytes on disk.
#'
#'   Another time that objects can be removed from the cache is when `get()` is
#'   called. If the target object is older than `max_age`, it will be removed
#'   and the cache will report it as a missing value.
#'
#' @section Eviction policies:
#'
#' If `max_n` or `max_size` are used, then objects will be removed
#' from the cache according to an eviction policy. The available eviction
#' policies are:
#'
#'   \describe{
#'     \item{`"lru"`}{
#'       Least Recently Used. The least recently used objects will be removed.
#'       This uses the filesystem's atime property. Some filesystems do not
#'       support atime, or have a very low atime resolution. The cache_mem will
#'       check for atime support, and if the filesystem does not support atime,
#'       a warning will be issued and the "fifo" policy will be used instead.
#'     }
#'     \item{`"fifo"`}{
#'       First-in-first-out. The oldest objects will be removed.
#'     }
#'   }
#'
#' @section Methods:
#'
#'  A disk cache object has the following methods:
#'
#'   \describe{
#'     \item{`get(key, missing)`}{
#'       Returns the value associated with `key`. If the key is not in the
#'       cache, then it evaluates the expression specified by `missing` and
#'       returns the value. If `missing` is specified here, then it will
#'       override the default that was set when the `cache_mem` object was
#'       created. See section Missing Keys for more information.
#'     }
#'     \item{`set(key, value)`}{
#'       Stores the `key`-`value` pair in the cache.
#'     }
#'     \item{`exists(key)`}{
#'       Returns `TRUE` if the cache contains the key, otherwise
#'       `FALSE`.
#'     }
#'     \item{`size()`}{
#'       Returns the number of items currently in the cache.
#'     }
#'     \item{`keys()`}{
#'       Returns a character vector of all keys currently in the cache.
#'     }
#'     \item{`reset()`}{
#'       Clears all objects from the cache.
#'     }
#'     \item{`destroy()`}{
#'       Clears all objects in the cache, and removes the cache directory from
#'       disk.
#'     }
#'     \item{`prune()`}{
#'       Prunes the cache, using the parameters specified by `max_size`,
#'       `max_age`, `max_n`, and `evict`.
#'     }
#'   }
#'
#' @inheritParams cache_disk
#'
#' @importFrom utils object.size
#' @export
cache_mem <- function(
  max_size = 1024 * 1024 ^ 2,
  max_age = Inf,
  max_n = Inf,
  evict = c("lru", "fifo"),
  missing = key_missing(),
  logfile = NULL)
{
  # ============================================================================
  # Initialization
  # ============================================================================
  if (!is.numeric(max_size)) stop("max_size must be a number. Use `Inf` for no limit.")
  if (!is.numeric(max_age))  stop("max_age must be a number. Use `Inf` for no limit.")
  if (!is.numeric(max_n))    stop("max_n must be a number. Use `Inf` for no limit.")
  cache_        <- fastmap()
  max_size_     <- max_size
  max_age_      <- max_age
  max_n_        <- max_n
  evict_        <- match.arg(evict)
  missing_      <- enquo(missing)
  logfile_      <- logfile

  # ============================================================================
  # Public methods
  # ============================================================================
  get <- function(key, missing = missing_) {
    log_(paste0('get: key "', key, '"'))
    validate_key(key)

    maybe_prune_single_(key)

    if (!exists(key)) {
      log_(paste0('get: key "', key, '" is missing'))
      missing <- as_quosure(missing)
      return(eval_tidy(missing))
    }

    log_(paste0('get: key "', key, '" found'))
    value <- cache_$get(key)$value
    value
  }

  set <- function(key, value) {
    log_(paste0('set: key "', key, '"'))
    validate_key(key)

    time <- as.numeric(Sys.time())

    # Only record size if we're actually using max_size for pruning.
    if (is.finite(max_size_)) {
      # Reported size is rough! See ?object.size.
      size <- as.numeric(object.size(value))
    } else {
      size <- NULL
    }

    cache_$set(key, list(
      key = key,
      value = value,
      size = size,
      mtime = time,
      atime = time
    ))
    prune()
    invisible(TRUE)
  }

  exists <- function(key) {
    validate_key(key)
    cache_$has(key)
  }

  keys <- function() {
    cache_$keys()
  }

  remove <- function(key) {
    log_(paste0('remove: key "', key, '"'))
    validate_key(key)
    cache_$remove(key)
    invisible(TRUE)
  }

  reset <- function() {
    log_(paste0('reset'))
    cache_$reset()
    invisible(TRUE)
  }

  prune <- function() {
    log_(paste0('prune'))
    info <- object_info_()

    # 1. Remove any objects where the age exceeds max age.
    if (is.finite(max_age_)) {
      time <- as.numeric(Sys.time())
      timediff <- time - info$mtime
      rm_idx <- timediff > max_age_
      if (any(rm_idx)) {
        log_(paste0("prune max_age: Removing ", paste(info$key[rm_idx], collapse = ", ")))
        cache_$remove(info$key[rm_idx])
        info <- info[!rm_idx, ]
      }
    }

    # Sort objects by priority, according to eviction policy. The sorting is
    # done in a function which can be called multiple times but only does
    # the work the first time.
    info_is_sorted <- FALSE
    ensure_info_is_sorted <- function() {
      if (info_is_sorted) return()

      if (evict_ == "lru") {
        info <<- info[order(info$atime, decreasing = TRUE), ]
      } else if (evict_ == "fifo") {
        info <<- info[order(info$mtime, decreasing = TRUE), ]
      } else {
        stop('Unknown eviction policy "', evict_, '"')
      }
      info_is_sorted <<- TRUE
    }

    # 2. Remove objects if there are too many.
    if (is.finite(max_n_) && nrow(info) > max_n_) {
      ensure_info_is_sorted()
      rm_idx <- seq_len(nrow(info)) > max_n_
      log_(paste0("prune max_n: Removing ", paste(info$key[rm_idx], collapse = ", ")))
      cache_$remove(info$key[rm_idx])
      info <- info[!rm_idx, ]
    }

    # 3. Remove objects if cache is too large.
    if (is.finite(max_size_) && sum(info$size) > max_size_) {
      ensure_info_is_sorted()
      cum_size <- cumsum(info$size)
      rm_idx <- cum_size > max_size_
      log_(paste0("prune max_size: Removing ", paste(info$key[rm_idx], collapse = ", ")))
      cache_$remove(info$key[rm_idx])
      info <- info[!rm_idx, ]
    }

    invisible(TRUE)
  }

  size <- function() {
    cache_$size()
  }

  info <- function() {
    list(
      max_size = max_size_,
      max_age = max_age_,
      max_n = max_n_,
      evict = evict_,
      missing = missing_,
      logfile = logfile_
    )
  }


  # ============================================================================
  # Private methods
  # ============================================================================

  # Prunes a single object if it exceeds max_age. If the object does not
  # exceed max_age, or if the object doesn't exist, do nothing.
  maybe_prune_single_ <- function(key) {
    if (!is.finite(max_age_)) return()

    obj <- cache_$get(key)
    if (is.null(obj)) return()

    timediff <- as.numeric(Sys.time()) - obj$mtime
    if (timediff > max_age_) {
      log_(paste0("pruning single object exceeding max_age: Removing ", key))
      cache_$remove(key)
    }
  }

  object_info_ = function() {
    keys <- cache_$keys()
    data.frame(
      key   = keys,
      size  = vapply(keys, function(key) cache_$get(key)$size,  0),
      mtime = vapply(keys, function(key) cache_$get(key)$mtime, 0),
      atime = vapply(keys, function(key) cache_$get(key)$atime, 0),
      stringsAsFactors = FALSE
    )
  }

  log_ <- function(text) {
    if (is.null(logfile_)) return()

    text <- paste0(format(Sys.time(), "[%Y-%m-%d %H:%M:%OS3] cache_mem "), text)
    cat(text, sep = "\n", file = logfile_, append = TRUE)
  }

  # ============================================================================
  # Returned object
  # ============================================================================
  structure(
    list(
      get = get,
      set = set,
      exists = exists,
      keys = keys,
      remove = remove,
      reset = reset,
      prune = prune,
      size = size,
      info = info
    ),
    class = c("cache_mem", "cache")
  )
}
