library(popcycle)

upload.filter <- function(cruise.name, file.name, evt, opp, stats, db=db.name) {
  df <- data.frame(cruise=cruise.name, file=file.name, opp_count=nrow(opp),
                   evt_count=nrow(evt), opp_evt_ratio=nrow(opp) / nrow(evt),
                   notch1=as.numeric(stats$notch1[1]), notch2=as.numeric(stats$notch2[1]), offset=0.0,
                   origin=median(evt$D2-evt$D1), width=0.5)
  con <- dbConnect(SQLite(), dbname=db)
  dbWriteTable(conn = con, name = "filter", value = df, row.names=FALSE, append=TRUE)
  dbDisconnect(con)
}

set.project.location("Rtestdir")
reset.db()
set.cruise.id("testcruise")
set.evt.location("testcruise")
evt.list <- get.evt.list()

for (f in evt.list[1:2]) {
  print(paste0("Filtering ", f))
  #try({
    stats <- find.filter.notch(c(f), offset=0, width=0.5, do.plot=F)
    evt <- readSeaflow(f, transform=FALSE)
    opp <- filter.notch(evt, offset=0, width=0.5)
    if (nrow(opp) > 0) {
      upload.opp(opp.to.db.opp(opp, "testcruise", f))
      upload.filter("testcruise", f, evt, opp, stats, db.name)
    }
  #}, silent=T)
}
