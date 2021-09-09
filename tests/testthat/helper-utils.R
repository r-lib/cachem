is_on_github_actions <- function() {
  nzchar(Sys.getenv("GITHUB_ACTIONS"))
}
