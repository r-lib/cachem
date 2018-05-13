dir_exists <- function(paths) {
  file.exists(paths) & file.info(paths)$isdir
}

dir_remove <- function(path) {
  for (p in path) {
    if (!dir_exists(p)) {
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
  if (grepl("[^a-z0-9]", key)) {
    stop("Invalid key: ", key, ". Only lowercase letters and numbers are allowed.")
  }
}
