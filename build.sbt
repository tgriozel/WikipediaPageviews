name := "wikipedia-pageviews"

version := "0.1"

scalaVersion := "2.12.11"

scalacOptions += "-Ypartial-unification"

val sparkVersion = "3.0.0"

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "com.google.inject" % "guice" % "4.2.3",
  "com.typesafe" % "config" % "1.4.0",
  "org.typelevel" %% "cats-effect" % "2.1.3",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.mockito" %% "mockito-scala-scalatest" % "1.13.9" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % Test
)

mainClass in (Compile, run) := Some("code.job.EntryPoint")

fork := true
