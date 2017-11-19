name := "Transpose"
version := "0.1"
scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "com.databricks" %% "spark-csv" % "1.5.0",
)
