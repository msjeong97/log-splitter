name := "log-splitter"

version := "0.1"

scalaVersion := "2.12.13"

libraryDependencies ++= Seq(
  // scala provided
  "org.apache.spark" %% "spark-sql" % "3.2.0" % Provided
)