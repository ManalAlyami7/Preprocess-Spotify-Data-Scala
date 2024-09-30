ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.3"


libraryDependencies += "com.opencsv" % "opencsv" % "5.5.2"


lazy val root = (project in file("."))
  .settings(
    name := "BigDataproject"
  )
