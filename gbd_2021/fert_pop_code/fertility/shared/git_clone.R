#' @title Clone repositories to new directory
#'
#' @description Helper function to clone repositories from GitHub and/or Stash
#'   to a fresh directory, from specified branches.
#'
#' @param clone_dir \[`character()`\] \cr
#'   Base directory to clone all repositories too. Cloned repositories will
#'   have path "{clone_dir}/{repo name}". This must be a fresh directory, and
#'   cannot already have specified repositories cloned to it.
#' @param github_repos \[`character()`\] \cr
#'   Named vector of `ihmeuw-demographics` github repositories, where the
#'   name is the repo name and the value is the branch name.
#'   Example: `c("demInternal" = "master")`.
#' @param stash_repos \[`character()`\] \cr
#'   Named vector of IHME stash/bitbucket repositories, where the name is
#'   "{project shorthand}/{repo name}" and the value is the branch name.
#'   Example: `c("dem/age-sex" = "master")`.
#'
#' @return Nothing. Repositories are cloned into "`clone_dir`/{repo name}".
#'
#' @details
#' Users must save git credentials:
#'   1. First time: create `~/.git-credentials` file with contents: \cr
#'      WEBSITE \cr
#'      WEBSITE \cr
#'      Other: open `~.git-credentials` and update your password.
#'   2. Run from the command line: \cr
#'      `git config --global credential.helper store`
#'   3. Run one test git command which interacts with the remote (stash/github).
#'      Step through the function running each component to receive a
#'      prompt to enter your username/password. The `demInternal::git_clone`
#'      function will not pass you any username/password prompts from the
#'      system, and instead will error out, so this check is a prerequisite.
#'   4. Enter username/password. At this point your credentials file may
#'      be automatically modified to replace special characters.
#'   5. From this point on, you should not be required to enter a username or
#'      password. You should run a git command to test that this is true.
#'
#' Go [here](https://git-scm.com/docs/git-credential-store) for more information.
#'
#' @examples
#' clone_dir <- tempdir()
#' github_repos <- c("model_template" = "main")
#' stash_repos <- c("dem/age-sex" = "main")
#' git_clone(clone_dir, github_repos, stash_repos)
#'
#' @export
git_clone <- function(clone_dir, github_repos = c(), stash_repos = c()) {

  # Validate ----------------------------------------------------------------

  assertthat::assert_that(
    file.exists("~/.git-credentials"),
    msg = "Missing git credentials (see `details` section of ??git_clone)."
  )

  assertthat::assert_that(
    dir.exists(clone_dir),
    msg = paste0("`clone_dir` ", clone_dir, " does not exist.")
  )

  # Clone -------------------------------------------------------------------

  # clone GitHub repos
  for (repo in names(github_repos)) {
    branchname <- github_repos[[repo]]
    fs::dir_create(fs::path(clone_dir, repo))
    error_code <- system(
      command = paste0("QUERY")
    )
    assertthat::assert_that(
      error_code == 0,
      msg = paste0("check '", repo, "' and '", branchname, "' are correct ",
                   "repository and branch names, and git credentials are valid.")
    )
  }

  # clone stash repos
  for (project_repo in names(stash_repos)) {
    branchname <- stash_repos[[project_repo]]
    repo <- strsplit(project_repo, "/")[[1]][2] # remove project prefix
    fs::dir_create(fs::path(clone_dir, repo))
    error_code <- system(
      command = paste0("QUERY")
    )
    assertthat::assert_that(
      error_code == 0,
      msg = paste0("check '", project_repo, "' and '", branchname, "' are correct ",
                   "repository and branch names, and git credentials are valid.")
    )
  }

}
