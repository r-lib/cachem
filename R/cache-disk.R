#' Create a disk cache object
#'
#' A disk cache object is a key-value store that saves the values as files in a
#' directory on disk. Objects can be stored and retrieved using the `get()` and
#' `set()` methods. Objects are automatically pruned from the cache according to
#' the parameters `max_size`, `max_age`, `max_n`, and `evict`.
#'
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
#' @section Cache pruning:
#'
#'   Cache pruning occurs when `set()` is called, or it can be invoked manually
#'   by calling `prune()`.
#'
#'   The disk cache will throttle the pruning so that it does not happen on
#'   every call to `set()`, because the filesystem operations for checking the
#'   status of files can be slow. Instead, it will prune once in every
#'   `prune_rate` calls to `set()`, or if at least 5 seconds have elapsed since
#'   the last prune occurred, whichever is first.
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
#'   If `max_n` or `max_size` are used, then objects will be removed from the
#'   cache according to an eviction policy. The available eviction policies are:
#'
#'   \describe{
#'     \item{`"lru"`}{
#'       Least Recently Used. The least recently used objects will be removed.
#'       This uses the filesystem's mtime property. When "lru" is used, each
#'       `get()` is called, it will update the file's mtime using
#'       [Sys.setFileTime()]. Note that on some platforms, the resolution of
#'       [Sys.setFileTime()] may be low, one or two seconds.
#'     }
#'     \item{`"fifo"`}{
#'       First-in-first-out. The oldest objects will be removed.
#'     }
#'   }
#'
#' Both of these policies use files' mtime. Note that some filesystems (notably
#' FAT) have poor mtime resolution. (atime is not used because support for atime
#' is worse than mtime.)
#'
#'
#' @section Sharing among multiple processes:
#'
#'   The directory for a cache_disk can be shared among multiple R processes. To
#'   do this, each R process should have a cache_disk object that uses the same
#'   directory. Each cache_disk will do pruning independently of the others, so
#'   if they have different pruning parameters, then one cache_disk may remove
#'   cached objects before another cache_disk would do so.
#'
#'   Even though it is possible for multiple processes to share a cache_disk
#'   directory, this should not be done on networked file systems, because of
#'   slow performance of networked file systems can cause problems. If you need
#'   a high-performance shared cache, you can use one built on a database like
#'   Redis, SQLite, mySQL, or similar.
#'
#'   When multiple processes share a cache directory, there are some potential
#'   race conditions. For example, if your code calls `exists(key)` to check if
#'   an object is in the cache, and then call `get(key)`, the object may be
#'   removed from the cache in between those two calls, and `get(key)` will
#'   throw an error. Instead of calling the two functions, it is better to
#'   simply call `get(key)`, and check that the returned object is not a
#'   `key_missing()` object, using `is.key_missing()`. This effectively tests
#'   for existence and gets the object in one operation.
#'
#'   It is also possible for one processes to prune objects at the same time
#'   that another processes is trying to prune objects. If this happens, you may
#'   see a warning from `file.remove()` failing to remove a file that has
#'   already been deleted.
#'
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
#'     \item{`remove(key)`}{
#'       Removes `key` from the cache, if it exists in the cache. If the key is
#'       not in the cache, this does nothing.
#'     }
#'     \item{`ttl(key)`}{
#'       Returns the time to live (seconds), using the parameters specified by
#'       `max_age`.
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
#' @param dir Directory to store files for the cache. If `NULL` (the default) it
#'   will create and use a temporary directory.
#' @param read_fn The function used to read the values from disk. If `NULL`
#'   (the default) it will use `readRDS`.
#' @param write_fn The function used to write the values from disk. If `NULL`
#'   (the default) it will use `writeRDS`.
#' @param extension The file extension to use for files on disk.
#' @param max_age Maximum age of files in cache before they are evicted, in
#'   seconds. Use `Inf` for no age limit.
#' @param max_size Maximum size of the cache, in bytes. If the cache exceeds
#'   this size, cached objects will be removed according to the value of the
#'   `evict`. Use `Inf` for no size limit. The default is 1 gigabyte.
#' @param max_n Maximum number of objects in the cache. If the number of objects
#'   exceeds this value, then cached objects will be removed according to the
#'   value of `evict`. Use `Inf` for no limit of number of items.
#' @param evict The eviction policy to use to decide which objects are removed
#'   when a cache pruning occurs. Currently, `"lru"` and `"fifo"` are supported.
#' @param destroy_on_finalize If `TRUE`, then when the cache_disk object is
#'   garbage collected, the cache directory and all objects inside of it will be
#'   deleted from disk. If `FALSE` (the default), it will do nothing when
#'   finalized.
#' @param missing A value to return when `get(key)` is called but the key is not
#'   present in the cache. The default is a [key_missing()] object. It is
#'   actually an expression that is evaluated each time there is a cache miss.
#'   See section Missing keys for more information.
#' @param prune_rate How often to prune the cache. See section Cache Pruning for
#'   more information.
#' @param warn_ref_objects Should a warning be emitted when a reference is
#'   stored in the cache? This can be useful because serializing and
#'   deserializing a reference object (such as environments and external
#'   pointers) can lead to unexpected behavior.
#' @param logfile An optional filename or connection object to where logging
#'   information will be written. To log to the console, use `stderr()` or
#'   `stdout()`.
#'
#' @return A disk caching object, with class `cache_disk`.
#' @importFrom rlang enquo eval_tidy as_quosure
#' @export
cache_disk <- function(
  dir = NULL,
  max_size = 1024 * 1024 ^ 2,
  max_age = Inf,
  max_n = Inf,
  evict = c("lru", "fifo"),
  destroy_on_finalize = FALSE,
  read_fn = NULL,
  write_fn = NULL,
  extension = ".rds",
  missing = key_missing(),
  prune_rate = 20,
  warn_ref_objects = FALSE,
  logfile = NULL
) {
  # ============================================================================
  # Constants
  # ============================================================================
  PRUNE_THROTTLE_TIME_LIMIT <- 5

  # ============================================================================
  # Logging
  # ============================================================================
  # This needs to be defined first, because it's used in initialization.
  log_ <- function(text) {
    if (is.null(logfile_)) return()

    text <- paste0(format(Sys.time(), "[%Y-%m-%d %H:%M:%OS3] cache_disk "), text)
    cat(text, sep = "\n", file = logfile_, append = TRUE)
  }

  # ============================================================================
  # Initialization
  # ============================================================================
  if (is.null(dir)) {
    dir <- tempfile("cache_disk-")
  }
  if (!is.numeric(max_size)) stop("max_size must be a number. Use `Inf` for no limit.")
  if (!is.numeric(max_age))  stop("max_age must be a number. Use `Inf` for no limit.")
  if (!is.numeric(max_n))    stop("max_n must be a number. Use `Inf` for no limit.")

  if (!dir.exists(dir)) {
    # log_(paste0("initialize: Creating ", dir))
    dir.create(dir, recursive = TRUE)
  }

  logfile_             <- logfile
  dir_                 <- normalizePath(dir)
  extension_           <- extension
  extension_regex_     <- sub("\\.(.*)", "\\\\.\\1$", extension)
  read_fn_             <- read_fn
  write_fn_            <- write_fn
  max_size_            <- max_size
  max_age_             <- max_age
  max_n_               <- max_n
  evict_               <- match.arg(evict)
  destroy_on_finalize_ <- destroy_on_finalize
  missing_             <- enquo(missing)
  prune_rate_          <- prune_rate

  destroyed_           <- FALSE


  if (is.null(read_fn_)) {
    read_fn_ <- readRDS
  }

  if (is.null(write_fn_)) {
    write_fn_ <- local({
      ref_object <- FALSE
      function(value, file) {
        on.exit({
          ref_object <<- TRUE
          unlink(temp_file)
        })
        temp_file <- paste0(file, "-temp-", random_hex(16))
        saveRDS(value, file = temp_file,
          refhook = function(x) {
            ref_object <<- TRUE
            NULL
          }
        )
        file.rename(temp_file, file)
        if (warn_ref_objects && ref_object) {
          log_(paste0('set: value is a reference object'))
          warning("A reference object was cached in a serialized format. The restored object may not work as expected.")
        }
      }
    })
  }

  # Start the prune throttle counter with a random number from 0-19. This is
  # so that, in the case where multiple cache_disk objects that point to the
  # same directory are created and discarded after just a few uses each,
  # pruning will still occur.
  prune_throttle_counter_ <- sample.int(prune_rate_, 1) - 1
  prune_last_time_        <- as.numeric(Sys.time())

  if (destroy_on_finalize_) {
    reg.finalizer(
      environment(),
      function(e) { e$destroy() }
    )
  }

  # ============================================================================
  # Public methods
  # ============================================================================
  get <- function(key, missing = missing_) {
    log_(paste0('get: key "', key, '"'))
    is_destroyed(throw = TRUE)
    validate_key(key)

    maybe_prune_single_(key)

    filename <- key_to_filename_(key)

    # Instead of calling exists() before fetching the value, just try to
    # fetch the value. This reduces the risk of a race condition when
    # multiple processes share a cache.
    read_error <- FALSE
    tryCatch(
      {
        value <- suppressWarnings(read_fn_(filename))
        if (evict_ == "lru"){
          Sys.setFileTime(filename, Sys.time())
        }
      },
      error = function(e) {
        read_error <<- TRUE
      }
    )
    if (read_error) {
      log_(paste0('get: key "', key, '" is missing'))
      missing <- as_quosure(missing)
      return(eval_tidy(missing))
    }

    log_(paste0('get: key "', key, '" found'))
    value
  }

  set <- function(key, value) {
    log_(paste0('set: key "', key, '"'))
    is_destroyed(throw = TRUE)
    validate_key(key)

    file <- key_to_filename_(key)

    save_error <- FALSE
    tryCatch(
      {
        write_fn_(value, file)
      },
      error = function(e) {
        save_error <<- TRUE
      }
    )
    if (save_error) {
      log_(paste0('set: key "', key, '" error'))
      stop('Error setting value for key "', key, '".')
    }

    prune_throttled_()
    invisible(TRUE)
  }

  exists <- function(key) {
    is_destroyed(throw = TRUE)
    validate_key(key)
    file.exists(key_to_filename_(key))
  }

  # Return all keys in the cache
  keys <- function() {
    is_destroyed(throw = TRUE)
    files <- dir(dir_, extension_regex_)
    sub(extension_regex_, "", files)
  }

  remove <- function(key) {
    log_(paste0('remove: key "', key, '"'))
    is_destroyed(throw = TRUE)
    validate_key(key)
    # Remove file; use unlink() instead of file.remove() because it won't
    # warn if the file doesn't exist.
    unlink(key_to_filename_(key))
    invisible(TRUE)
  }

  reset <- function() {
    log_(paste0('reset'))
    is_destroyed(throw = TRUE)
    file.remove(dir(dir_, extension_regex_, full.names = TRUE))
    invisible(TRUE)
  }

  prune <- function() {
    # TODO: It would be good to add parameters `n` and `size`, so that the
    # cache can be pruned to `max_n - n` and `max_size - size` before adding
    # an object. Right now we prune after adding the object, so the cache
    # can temporarily grow past the limits. The reason we don't do this now
    # is because it is expensive to find the size of the serialized object
    # before adding it.

    log_('prune')
    is_destroyed(throw = TRUE)

    current_time <- Sys.time()

    filenames <- dir(dir_, extension_regex_, full.names = TRUE)
    info <- file.info(filenames, extra_cols = FALSE)
    info <- info[info$isdir == FALSE, ]
    info$name <- rownames(info)
    rownames(info) <- NULL
    # Files could be removed between the dir() and file.info() calls. The
    # entire row for such files will have NA values. Remove those rows.
    info <- info[!is.na(info$size), ]

    # 1. Remove any files where the age exceeds max age.
    if (is.finite(max_age_)) {
      timediff <- as.numeric(current_time - info$mtime, units = "secs")
      rm_idx <- timediff > max_age_
      if (any(rm_idx)) {
        log_(paste0("prune max_age: Removing ", paste(info$name[rm_idx], collapse = ", ")))
        rm_success <- file.remove(info$name[rm_idx])
        # This maps rm_success back into the TRUEs in the rm_idx vector.
        # If (for example) rm_idx is c(F,T,F,T,T) and rm_success is c(T,F,T),
        # then this line modifies rm_idx to be c(F,T,F,F,T).
        rm_idx[rm_idx] <- rm_success
        info <- info[!rm_idx, ]
      }
    }

    # Sort objects by priority. The sorting is done in a function which can be
    # called multiple times but only does the work the first time.
    info_is_sorted <- FALSE
    ensure_info_is_sorted <- function() {
      if (info_is_sorted) return()

      info <<- info[order(info$mtime, decreasing = TRUE), ]
      info_is_sorted <<- TRUE
    }

    # 2. Remove files if there are too many.
    if (is.finite(max_n_) && nrow(info) > max_n_) {
      ensure_info_is_sorted()
      rm_idx <- seq_len(nrow(info)) > max_n_
      log_(paste0("prune max_n: Removing ", paste(info$name[rm_idx], collapse = ", ")))
      rm_success <- file.remove(info$name[rm_idx])
      rm_idx[rm_idx] <- rm_success
      info <- info[!rm_idx, ]
    }

    # 3. Remove files if cache is too large.
    if (is.finite(max_size_) && sum(info$size) > max_size_) {
      ensure_info_is_sorted()
      cum_size <- cumsum(info$size)
      rm_idx <- cum_size > max_size_
      log_(paste0("prune max_size: Removing ", paste(info$name[rm_idx], collapse = ", ")))
      rm_success <- file.remove(info$name[rm_idx])
      rm_idx[rm_idx] <- rm_success
      info <- info[!rm_idx, ]
    }

    prune_last_time_ <<- as.numeric(current_time)

    invisible(TRUE)
  }

  size <- function() {
    is_destroyed(throw = TRUE)
    length(dir(dir_, extension_regex_))
  }

  info <- function() {
    list(
      dir = dir_,
      max_size = max_size_,
      max_age = max_age_,
      max_n = max_n_,
      evict = evict_,
      destroy_on_finalize = destroy_on_finalize_,
      missing = missing_,
      prune_rate = prune_rate,
      logfile = logfile_,
      prune_throttle_counter = prune_throttle_counter_,
      prune_last_time = as.POSIXct(prune_last_time_, origin = "1970-01-01")
    )
  }

  ttl <- function(key) {
    is_destroyed(throw = TRUE)
    validate_key(key)
    maybe_prune_single_(key)
    if (is.finite(max_age_)) {
      time <- as.numeric(Sys.time())
      mtime <- as.numeric(file.info(key_to_filename_(key))$mtime, units = "secs")
      age <- mtime + max_age_ - time
      if (!isTRUE(age > 0)) age <- NA_real_
      age
    } else Inf
  }

  destroy <- function() {
    if (is_destroyed()) {
      return(invisible(FALSE))
    }

    log_(paste0("destroy: Removing ", dir_))
    # First create a sentinel file so that other processes sharing this
    # cache know that the cache is to be destroyed. This is needed because
    # the recursive unlink is not atomic: another process can add a file to
    # the directory after unlink starts removing files but before it removes
    # the directory, and when that happens, the directory removal will fail.
    file.create(file.path(dir_, "__destroyed__"))
    # Remove all the extension files. This will not remove the setinel file.
    file.remove(dir(dir_, extension_regex_, full.names = TRUE))
    # Next remove dir recursively, including sentinel file.
    unlink(dir_, recursive = TRUE)
    destroyed_ <<- TRUE
    invisible(TRUE)
  }

  is_destroyed <- function(throw = FALSE) {
    if (!dir.exists(dir_) ||
        file.exists(file.path(dir_, "__destroyed__")))
    {
      # It's possible for another process to destroy a shared cache directory
      destroyed_ <<- TRUE
    }

    if (throw) {
      if (destroyed_) {
        stop("Attempted to use cache which has been destroyed:\n  ", dir_)
      }

    } else {
      destroyed_
    }
  }

  # ============================================================================
  # Private methods
  # ============================================================================
  key_to_filename_ <- function(key) {
    validate_key(key)
    # Additional validation. This 80-char limit is arbitrary, and is
    # intended to avoid hitting a filename length limit on Windows.
    if (nchar(key) > 80) {
      stop("Invalid key: key must have fewer than 80 characters.")
    }
    file.path(dir_, paste0(key, extension_))
  }

  # A wrapper for prune() that throttles it, because prune() can be expensive
  # due to filesystem operations. This function will prune only once every
  # `prune_rate` times it is called, or if it has been more than 5 seconds since
  # the last time the cache was actually pruned, whichever is first. In the
  # future, the behavior may be customizable.
  prune_throttled_ <- function() {
    # Count the number of times prune() has been called.
    prune_throttle_counter_ <<- prune_throttle_counter_ + 1

    if (prune_throttle_counter_ >= prune_rate_ ||
        as.numeric(Sys.time()) - prune_last_time_ > PRUNE_THROTTLE_TIME_LIMIT)
    {
      prune()
      prune_throttle_counter_ <<- 0
    }
  }

  # Prunes a single object if it exceeds max_age. If the object does not
  # exceed max_age, or if the object doesn't exist, do nothing.
  maybe_prune_single_ <- function(key) {
    # obj <- cache_[[key]]
    # if (is.null(obj)) return()
    filepath <- file.path(dir_, paste0(key, extension_))
    info <- file.info(filepath, extra_cols = FALSE)
    if (is.na(info$mtime)) return()

    timediff <- as.numeric(Sys.time()) - as.numeric(info$mtime)
    if (timediff > max_age_) {
      log_(paste0("pruning single object exceeding max_age: Removing ", key))
      unlink(filepath)
    }
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
      ttl = ttl,
      reset = reset,
      prune = prune,
      size = size,
      destroy = destroy,
      is_destroyed = is_destroyed,
      info = info
    ),
    class = c("cache_disk", "cachem")
  )
}
