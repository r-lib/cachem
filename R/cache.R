#' @export
format.cache <- function(x, ...) {
  paste0(
    paste0("<", class(x), ">", collapse= " "), "\n",
    "  Methods:\n",
    paste0(
      "    ", format_methods(x),
      collapse ="\n"
    )
  )
}

format_methods <- function(x) {
  vapply(seq_along(x),
    function(i) {
      name <- names(x)[i]
      f <- x[[i]]
      if (is.function(f)) {
        paste0(name, "(", format_args(f), ")")
      } else {
        name
      }
    }, character(1)
  )
}

format_args <- function(x) {
  nms <- names(formals(x))
  vals <- as.character(formals(x))
  args <- mapply(nms, vals, FUN = function(name, value) {
    if (value == "") {
      name
    } else {
      paste0(name, " = ", value)
    }
  })
  paste(args, collapse = ", ")
}

#' @export
print.cache <- function(x, ...) {
  cat(format(x, ...))
}
