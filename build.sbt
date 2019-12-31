import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "io.strixvaria",
      scalaVersion := "2.12.6",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "strix-csv",
    libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.0" % Test
  )
