hex_digits <- c("0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
                "a", "b", "c", "d", "e", "f")

random_hex <- function(digits = 16) {
  with_private_seed({
    paste(sample(hex_digits, digits, replace = TRUE), collapse = "")
  })
}


dir_remove <- function(path) {
  for (p in path) {
    if (!dir.exists(p)) {
      stop("Cannot remove non-existent directory ", p, ".")
    }
    if (length(dir(p, all.files = TRUE, no.. = TRUE)) != 0) {
      stop("Cannot remove non-empty directory ", p, ".")
    }
    result <- unlink(p, recursive = TRUE)
    if (result == 1) {
      stop("Error removing directory ", p, ".")
    }
  }
}

absolute_path <- function(path) {
  norm_path <- normalizePath(path, mustWork = FALSE)
  if (path == norm_path) {
    file.path(getwd(), path)
  } else {
    norm_path
  }
}

validate_key <- function(key) {
  # This C function does the same as `grepl("[^a-z0-9_-]")`, but faster.
  .Call(C_validate_key, key)
}


.globals <- new.env(parent = emptyenv())
.globals$own_seed <- NULL

# Evaluate an expression using cachem's own private stream of randomness (not
# affected by set.seed).
with_private_seed <- function(expr) {
  # Save the old seed if present.
  if (exists(".Random.seed", envir = .GlobalEnv, inherits = FALSE)) {
    has_orig_seed <- TRUE
    orig_seed <- .GlobalEnv$.Random.seed
  } else {
    has_orig_seed <- FALSE
  }

  # Swap in the private seed.
  if (is.null(.globals$own_seed)) {
    if (has_orig_seed) {
      # Move old seed out of the way if present.
      rm(.Random.seed, envir = .GlobalEnv, inherits = FALSE)
    }
  } else {
    .GlobalEnv$.Random.seed <- .globals$own_seed
  }

  # On exit, save the modified private seed, and put the old seed back.
  on.exit({
    .globals$own_seed <- .GlobalEnv$.Random.seed

    if (has_orig_seed) {
      .GlobalEnv$.Random.seed <- orig_seed
    } else {
      rm(.Random.seed, envir = .GlobalEnv, inherits = FALSE)
    }
  })

  expr
}

.onLoad <- function(libname, pkgname) {
  # R's lazy-loading package scheme causes the private seed to be cached in the
  # package itself, making our PRNG completely deterministic. This line resets
  # the private seed during load.
  with_private_seed(set.seed(NULL))
}
