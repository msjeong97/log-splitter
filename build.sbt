name := "log-splitter"

version := "0.1"

scalaVersion := "2.12.13"

libraryDependencies ++= Seq(
  // scala provided
  "org.apache.spark" %% "spark-sql" % "3.2.0" % Provided,

  // third-party library
  "eu.bitwalker" % "UserAgentUtils" % "1.21",

  // scala test
  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}