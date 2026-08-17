val scala3Version = "3.4.2"

ThisBuild / organization := "io.github.ccerdadiaz"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := scala3Version

val commonScalacOptions = Seq(
  "-deprecation",
  "-feature"
)

lazy val core = (project in file("core"))
  .settings(
    name := "saga-graph-core",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    ),
    scalacOptions ++= commonScalacOptions
  )

lazy val storeSqlite = (project in file("store-sqlite"))
  .dependsOn(core)
  .settings(
    name := "saga-graph-store-sqlite",
    libraryDependencies ++= Seq(
      "org.xerial" % "sqlite-jdbc" % "3.46.0.0",
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    ),
    scalacOptions ++= commonScalacOptions
  )

lazy val examples = (project in file("examples"))
  .dependsOn(core, storeSqlite)
  .settings(
    name := "saga-graph-examples",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.18" % Test,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "ch.qos.logback" % "logback-classic" % "1.5.6",
      "org.eclipse.jetty" % "jetty-server" % "11.0.20",
      "org.eclipse.jetty" % "jetty-servlet" % "11.0.20",
      "com.lihaoyi" %% "upickle" % "3.3.1"
    ),
    scalacOptions ++= commonScalacOptions
  )

lazy val root = (project in file("."))
  .aggregate(core, storeSqlite, examples)
  .settings(
    publish / skip := true
  )
