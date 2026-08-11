val scala3Version = "3.4.2"

ThisBuild / organization := "io.github.ccerdadiaz"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := scala3Version

lazy val core = (project in file("core"))
  .settings(
    name := "saga-graph-core",
    // Cero dependencias externas — filosofía Unix
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    ),
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
    )
  )

