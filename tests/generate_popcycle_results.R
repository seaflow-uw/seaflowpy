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
vct.dir <- file.path(wd, "vct")

file.copy(paramsdb, db, copy.mode=F)
make.popcycle.db(db)
filter.evt.files(db, cruise, evt.dir, get.evt.files(evt.dir), opp.dir)
classify.opp.files(db, cruise, opp.dir, get.opp.files(db), vct.dir)
