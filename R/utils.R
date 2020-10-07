hex_digits <- c("0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
                "a", "b", "c", "d", "e", "f")

random_hex <- function(digits = 16) {
  paste(sample(hex_digits, digits, replace = TRUE), collapse = "")
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
  # This C function does the same as `grepl("[^a-z0-9]")`, but faster.
  .Call(C_validate_key, key)
}
