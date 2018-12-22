#!/usr/bin/env Rscript
library(popcycle)

args = commandArgs(trailingOnly=TRUE)
if (length(args) < 2) {
  stop("generate_popcycle_results.R testdir outputdir", call.=FALSE)
} else {
  if (file.info(args[1])$isdir) {
    testdir <- args[1]
  } else {
    stop(paste0("Error: argument ", args[1], " is not a directory"), call.=FALSE)
  }
  wd <- args[2]
  if (! file.exists(wd)) {
    dir.create(wd)
  }
}

cruise <- "testcruise"
paramsdb <- file.path(testdir, "testcruise_paramsonly.db")
db <- file.path(wd, "testcruise.db")
evt.dir <- file.path(testdir, "testcruise_evt")
opp.dir <- file.path(wd, "opp")

file.copy(paramsdb, db, copy.mode=F)
get.filter.params.latest(db)
evt_files <- get.evt.files(evt.dir)
# Filter files and skip the last EVT file (not in SFL file)
filter.evt.files(db, evt.dir, evt_files[1:length(evt_files)-1], opp.dir)
