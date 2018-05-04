#' @export
DiskCache <- R6::R6Class("DiskCache",
  public = list(
    initialize = function(dir = tempfile("DiskCache-"),
                          max_size = 5 * 1024^2,
                          max_age = Inf,
                          discard = c("oldest", "newest"),
                          timetype = c("ctime", "atime", "mtime"),
                          reset_on_finalize = TRUE)
    {
      if (!dir_exists(dir)) {
        message("Creating ", dir)
        dir.create(dir, recursive = TRUE, mode = "0700")
        private$dir_was_created <- TRUE
      }
      private$dir               <- absolute_path(dir)
      private$max_size          <- max_size
      private$max_age           <- max_age
      private$discard           <- match.arg(discard)
      private$timetype          <- match.arg(timetype)
      private$reset_on_finalize <- reset_on_finalize
    },

    # TODO:
    # Should call has() and return some sentinal object if not present?
    # Should be atomic to avoid race conditions with other processes.
    # Reduce pruning for mset/mget
    get = function(key) {
      validate_key(key)
      if (!self$has(key)) {
        stop("Key not available: ", key)
      }
      value <- readRDS(private$key_to_filename(key))
      self$prune()
      value
    },

    mget = function(keys) {
      lapply(keys, self$get)
    },

    set = function(key, value) {
      validate_key(key)
      self$prune()
      saveRDS(value, file = private$key_to_filename(key))
      invisible(self)
    },

    mset = function(..., .list = NULL) {
      args <- c(list(...), .list)
      if (length(args) == 0) {
        return()
      }

      arg_names <- names(args)
      if (is.null(arg_names) || any(!nzchar(arg_names))) {
        stop("All items must be named")
      }

      mapply(self$set, arg_names, args)
    },

    has = function(key) {
      validate_key(key)
      file.exists(private$key_to_filename(key))
    },

    # Return all keys in the cache
    keys = function() {
      files <- dir(private$dir, "*.rds")
      sub("\\.rds$", "", files)
    },

    remove = function(key) {
      validate_key(key)
      file.remove(private$key_to_filename(key))
      invisible(self)
    },

    reset = function() {
      file.remove(dir(private$dir, "*.rds", full.names = TRUE))
      invisible(self)
    },

    prune = function() {
      files <- file.info(dir(private$dir, "*.rds", full.names = TRUE))
      files <- files[files$isdir == FALSE, ]
      files$name <- rownames(files)
      rownames(files) <- NULL

      time <- Sys.time()
      # Remove any files where the age exceeds max age.
      files$timediff <- as.numeric(Sys.time() - files[[private$timetype]], units = "secs")
      files$rm <- files$timediff > private$max_age
      if (any(files$rm)) {
        message("Removing ", paste(files$name[files$rm], collapse = ", "))
      }
      file.remove(files$name[files$rm])

      # Remove rows of files that were deleted
      files <- files[!files$rm, ]

      # Sort the files by time, get a cumulative sum of size, and remove any
      # files where the cumlative size exceeds max_size.
      if (sum(files$size) > private$max_size) {
        sort_decreasing <- (private$discard == "oldest")

        files <- files[order(files[[private$timetype]], decreasing = sort_decreasing), ]
        files$cum_size <- cumsum(files$size)
        files$rm <- files$cum_size > private$max_size
        if (any(files$rm)) {
          message("Removing ", paste(files$name[files$rm], collapse = ", "))
        }
        file.remove(files$name[files$rm])
      }
      invisible(self)
    },

    size = function() {
      length(dir(private$dir, "*.rds"))
    },

    # TODO
    # Resets the cache and destroys the containing folder so that no
    # one else who shares the data back end can use it anymore.
    destroy = function() {
    },

    finalize = function() {
      if (private$reset_on_finalize) {
        self$reset()
        if (private$dir_was_created) {
          message("Removing ", private$dir)
          dir_remove(private$dir)
        }
      }
    }
  ),

  private = list(
    dir = NULL,
    max_age = NULL,
    max_size = NULL,
    discard = NULL,
    timetype = NULL,
    dir_was_created = FALSE,
    reset_on_finalize = NULL,

    key_to_filename = function(key) {
      if (! (is.character(key) && length(key)==1) ) {
        stop("Key must be a character vector of length 1.")
      }
      file.path(private$dir, paste0(key, ".rds"))
    }
  )
)
