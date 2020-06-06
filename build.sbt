import NativePackagerHelper._
import sbt.Keys.libraryDependencies

version := "0.1.0-SNAPSHOT"
scalaVersion := "2.11.12"

parallelExecution in Test := false
parallelExecution in IntegrationTest := false
enablePlugins(JavaAppPackaging)

lazy val app = project
  .in(file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    inThisBuild(List(
      scalaVersion := "2.11.12"
    )),
    name := "TranscationsWithSpark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.4" ,
      "org.apache.spark" %% "spark-sql" % "2.4.4",
      "org.apache.spark" %% "spark-avro" % "2.4.4",
      "org.apache.hudi" %% "hudi-spark-bundle" % "0.5.2-incubating",
      "org.apache.iceberg" % "iceberg-spark-runtime" % "0.8.0-incubating",
      "org.scalatest" %% "scalatest" % "3.0.5" % Test,
      "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0" % Test,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.0" % Test,
      "org.scalamock" %% "scalamock" % "4.4.0" % Test,
      "org.scalatest" %% "scalatest" % "3.0.5" % IntegrationTest,
      "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0" % IntegrationTest,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.0" % IntegrationTest
    ),
    mappings in Universal ++= directory("conf/"),
    mappings in Universal ++= directory("scripts/"),
    mappings in Universal ++= directory("src/main/resources/")
  )
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}

