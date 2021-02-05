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
#' @return A memory caching object, with class `cache_mem`.
#' @importFrom utils object.size
#' @export
cache_mem <- function(
  max_size = 512 * 1024 ^ 2,
  max_age = Inf,
  max_n = Inf,
  evict = c("lru", "fifo"),
  missing = key_missing(),
  logfile = NULL)
{
  # ============================================================================
  # Constants
  # ============================================================================
  DEBUG         <- TRUE
  INITIAL_SIZE  <- 64L
  COMPACT_LIMIT <- 256L
  COMPACT_MULT  <- 2
  # If TRUE, the data will be kept in the correct atime order each time get() is
  # called, though the metadata log will grow by one entry each time. If FALSE,
  # the metadata entry will be kept in place, but the atimes will not be kept in
  # order; instead, the metadata will be sorted by atime each time prune() is
  # called. The overall behavior is the same, but there are somewhat different
  # performance characteristics.
  MAINTAIN_ATIME_SORT <- FALSE

  # ============================================================================
  # Initialization
  # ============================================================================
  if (!is.numeric(max_size)) stop("max_size must be a number. Use `Inf` for no limit.")
  if (!is.numeric(max_age))  stop("max_age must be a number. Use `Inf` for no limit.")
  if (!is.numeric(max_n))    stop("max_n must be a number. Use `Inf` for no limit.")

  max_size_     <- max_size
  max_age_      <- max_age
  max_n_        <- max_n
  evict_        <- match.arg(evict)
  missing_      <- enquo(missing)
  logfile_      <- logfile

  PRUNE_BY_SIZE <- is.finite(max_size_)
  PRUNE_BY_AGE  <- is.finite(max_age_)
  PRUNE_BY_N    <- is.finite(max_n_)

  # ============================================================================
  # Internal state
  # ============================================================================
  # The keys, values, and metadata are stored in columnar format. The vectors
  # key_, value_, size_, mtime_, and atime_ are the columns. Separate vectors
  # are used instead of a data frame, because operations for modifying and
  # growing vectors are much faster than the same operations on data frames.
  #
  # It uses a column-first format because a row-first format is much slower for
  # doing the manipulations and computations that are needed for pruning, such
  # as sorting by atime, and calculating a cumulative sum of sizes.
  #
  # For fast get() performance, there is also key_idx_map_, which maps between
  # the key, and the "row" index in our "data frame".
  #
  # An older version of this code stored the value along with metadata (size,
  # mtime, and atime) in a fastmap object, but this had poor performance for
  # pruning operations. This is because, for pruning, it needs to fetch the
  # metadata for all objects, then sort by atime (if evict="lru"), then take a
  # cumulative sum of sizes. Fetching the metadata for all objects was slow, as
  # was converting the resulting row-first data into column-first data. The
  # current column-first approach is much, much faster.
  key_idx_map_  <- fastmap()

  key_          <- rep_len(NA_character_, INITIAL_SIZE)
  value_        <- vector("list",         INITIAL_SIZE)
  size_         <- rep_len(NA_real_,      INITIAL_SIZE)
  mtime_        <- rep_len(NA_real_,      INITIAL_SIZE)
  atime_        <- rep_len(NA_real_,      INITIAL_SIZE)

  total_n_      <- 0L  # Total number of items
  total_size_   <- 0   # Total number of bytes used
  last_idx_     <- 0L  # Most recent (and largest) index used


  # ============================================================================
  # Public methods
  # ============================================================================
  get <- function(key, missing = missing_) {
    log_(paste0('get: key "', key, '"'))
    validate_key(key)

    maybe_prune_single_(key)

    if (!key_idx_map_$has(key)) {
      log_(paste0('get: key "', key, '" is missing'))
      missing <- as_quosure(missing)
      return(eval_tidy(missing))
    }

    log_(paste0('get: key "', key, '" found'))

    update_atime_(key)

    idx <- key_idx_map_$get(key)
    value_[[idx]]
  }

  set <- function(key, value) {
    log_(paste0('set: key "', key, '"'))
    validate_key(key)

    time <- as.numeric(Sys.time())

    if (PRUNE_BY_SIZE) {
      # Reported size is rough! See ?object.size.
      size <- as.numeric(object.size(value))
      total_size_ <<- total_size_ + size
    }

    old_idx <- key_idx_map_$get(key)

    # We'll set this to TRUE if we need to append to the data; FALSE if we can
    # modify the existing entry in place.
    append <- NULL

    if (!is.null(old_idx)) {
      # If there's an existing entry with this key, clear out its row, because
      # we'll be appending a new one later.
      if (PRUNE_BY_SIZE) {
        total_size_ <<- total_size_ - size_[old_idx]
      }

      if (MAINTAIN_ATIME_SORT  &&  old_idx != last_idx_) {
        append <- TRUE

        key_  [old_idx] <<- NA_character_
        value_[old_idx] <<- list(NULL)
        size_ [old_idx] <<- NA_real_
        mtime_[old_idx] <<- NA_real_
        atime_[old_idx] <<- NA_real_

      } else {
        append <- FALSE
      }

    } else {
      append <- TRUE
      total_n_ <<- total_n_ + 1L
    }

    if (append) {
      # If we're appending, update the last_idx_ and use it for storage.
      last_idx_ <<- last_idx_ + 1L
      key_idx_map_$set(key, last_idx_)
      new_idx <- last_idx_

    } else {
      # Not appending; replace the old item in place.
      new_idx <- old_idx
    }

    key_  [new_idx]   <<- key
    value_[[new_idx]] <<- value
    size_ [new_idx]   <<- size
    mtime_[new_idx]   <<- time
    atime_[new_idx]   <<- time

    prune()

    invisible(TRUE)
  }

  exists <- function(key) {
    validate_key(key)
    key_idx_map_$has(key)
  }

  keys <- function() {
    key_idx_map_$keys()
  }

  remove <- function(key) {
    log_(paste0('remove: key "', key, '"'))
    validate_key(key)
    remove_(key)
    invisible(TRUE)
  }

  reset <- function() {
    log_(paste0('reset'))
    key_idx_map_$reset()
    invisible(TRUE)
  }

  prune <- function() {
    log_(paste0('prune'))

    # Quick check to see if we need to prune
    if ((!PRUNE_BY_SIZE || total_size_ <= max_size_) &&
        (!PRUNE_BY_N    || total_n_    <= max_n_   ) &&
        (!PRUNE_BY_AGE))
    {
      return(invisible(TRUE))
    }

    info <- get_metadata_()

    if (DEBUG) {
      # Sanity checks
      if (PRUNE_BY_SIZE && sum(info$size) != total_size_) {
        stop("Size mismatch")
      }
      if (length(info$key) != total_n_) {
        stop("Count mismatch")
      }
    }

    # 1. Remove any objects where the age exceeds max age.
    if (PRUNE_BY_AGE) {
      time <- as.numeric(Sys.time())
      timediff <- time - info$mtime
      rm_idx <- timediff > max_age_
      if (any(rm_idx)) {
        log_(paste0("prune max_age: Removing ", paste(info$key[rm_idx], collapse = ", ")))
        remove_(info$key[rm_idx])

        # Trim all the vectors (need to do each individually since we're using a
        # list of vectors instead of a data frame, for performance).
        info$key   <- info$key  [!rm_idx]
        info$size  <- info$size [!rm_idx]
        info$mtime <- info$mtime[!rm_idx]
        info$atime <- info$atime[!rm_idx]
      }
    }

    # 2. Remove objects if there are too many.
    if (PRUNE_BY_N && length(info$key) > max_n_) {
      rm_idx <- seq_along(info$key) > max_n_
      log_(paste0("prune max_n: Removing ", paste(info$key[rm_idx], collapse = ", ")))
      remove_(info$key[rm_idx])

      info$key   <- info$key  [!rm_idx]
      info$size  <- info$size [!rm_idx]
      info$mtime <- info$mtime[!rm_idx]
      info$atime <- info$atime[!rm_idx]
    }

    # 3. Remove objects if cache is too large.
    if (PRUNE_BY_SIZE && sum(info$size) > max_size_) {
      cum_size <- cumsum(info$size)
      rm_idx <- cum_size > max_size_
      log_(paste0("prune max_size: Removing ", paste(info$key[rm_idx], collapse = ", ")))
      remove_(info$key[rm_idx])

      # No need to trim vectors this time, since this is the last pruning step.
    }

    invisible(TRUE)
  }

  size <- function() {
    if (DEBUG) {
      if (key_idx_map_$size() != total_n_) stop("n mismatch")
    }
    total_n_
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

  # Called when get() with lru. If fifo, no need to update
  update_atime_ <- function(key) {
    if (evict_ != "lru") return()

    time <- as.numeric(Sys.time())
    idx <- key_idx_map_$get(key)

    if (is.null(idx)) {
      stop("Can't update atime because entry doesn't exist")
    }

    if (MAINTAIN_ATIME_SORT) {
      if (idx == last_idx_) {
        # last_idx_ entry; simply update time
        atime_[idx] <<- time
      } else {
        # "Move" this entry to the end.
        last_idx_ <<- last_idx_ + 1L
        # Add new entry to end
        key_idx_map_$set(key, last_idx_)
        key_  [last_idx_]   <<- key
        value_[[last_idx_]] <<- value_[[idx]]
        size_ [last_idx_]   <<- size_ [idx]
        mtime_[last_idx_]   <<- mtime_[idx]
        atime_[last_idx_]   <<- time

        # Clear out old entry
        key_  [idx] <<- NA_character_
        value_[idx] <<- list(NULL)
        size_ [idx] <<- NA_real_
        mtime_[idx] <<- NA_real_
        atime_[idx] <<- NA_real_
      }

    } else {
      atime_[idx] <<- time
    }

  }


  remove_ <- function(keys) {
    if (length(keys) == 1) {
      remove_one_(keys)
    } else {
      vapply(keys, remove_one_, TRUE)
    }

    if (last_idx_ > COMPACT_LIMIT  &&  last_idx_ > total_n_ * COMPACT_MULT) {
      compact_()
    }
  }

  remove_one_ <- function(key) {
    idx <- key_idx_map_$get(key)

    if (is.null(idx)) {
      return()
    }

    # Overall n and size bookkeeping
    total_n_ <<- total_n_ - 1L
    if (PRUNE_BY_SIZE) {
      total_size_ <<- total_size_ - size_[idx]
    }

    # Clear out entry
    key_  [idx] <<- NA_character_
    value_[idx] <<- list(NULL)
    size_ [idx] <<- NA_real_
    mtime_[idx] <<- NA_real_
    atime_[idx] <<- NA_real_

    key_idx_map_$remove(key)
  }

  # Prunes a single object if it exceeds max_age. If the object does not
  # exceed max_age, or if the object doesn't exist, do nothing.
  maybe_prune_single_ <- function(key) {
    if (!PRUNE_BY_AGE) return()

    time <- as.numeric(Sys.time())
    idx <- key_idx_map_$get(key)
    if (time - mtime[idx] > max_age_) {
      log_(paste0("pruning single object exceeding max_age: Removing ", key))
      remove_(key)
    }
  }

  compact_ <- function() {
    from_idxs <- key_[seq_len(last_idx_)]
    from_idxs <- !is.na(from_idxs)
    from_idxs <- which(from_idxs)

    if (DEBUG) stopifnot(total_n_ == length(from_idxs))

    if (total_n_ == 0L) {
      message("nothing to compact")
      return()
    }

    new_size <- ceiling(total_n_ * COMPACT_MULT)

    # Allocate new vectors for metadata.
    new_key_   <- rep_len(NA_character_, new_size)
    new_value_ <- vector("list",         new_size)
    new_size_  <- rep_len(NA_real_,      new_size)
    new_mtime_ <- rep_len(NA_real_,      new_size)
    new_atime_ <- rep_len(NA_real_,      new_size)

    # Copy (and compact, removing gaps) from old vectors to new ones.
    to_idxs <- seq_len(total_n_)
    new_key_  [to_idxs] <- key_  [from_idxs]
    new_value_[to_idxs] <- value_[from_idxs]
    new_size_ [to_idxs] <- size_ [from_idxs]
    new_mtime_[to_idxs] <- mtime_[from_idxs]
    new_atime_[to_idxs] <- atime_[from_idxs]

    # Replace old vectors with new ones.
    key_   <<- new_key_
    value_ <<- new_value_
    size_  <<- new_size_
    mtime_ <<- new_mtime_
    atime_ <<- new_atime_

    # Update the index values in the key-index map.
    args <- to_idxs
    names(args) <- key_[to_idxs]
    key_idx_map_$mset(.list = args)

    last_idx_ <<- total_n_
  }

  # Returns data frame of info, with gaps removed.
  # If evict=="lru", this will be sorted by atime.
  # If evict=="fifo", this will be sorted by mtime.
  get_metadata_ <- function() {
    idxs <- !is.na(mtime_[seq_len(last_idx_)])
    idxs <- which(idxs)

    if (!MAINTAIN_ATIME_SORT) {
      if (evict_ == "lru") {
        idxs <- idxs[order(atime_[idxs])]
      } else {
        idxs <- idxs[order(mtime_[idxs])]
      }
    }

    idxs <- rev(idxs)

    # Return a list -- this basically same structure as a data frame, but
    # we're using a plain list to avoid data frame slowness
    list(
      key   = key_  [idxs],
      size  = size_ [idxs],
      mtime = mtime_[idxs],
      atime = atime_[idxs]
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
    class = c("cache_mem", "cachem")
  )
}
