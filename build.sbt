
name := "wiki_data_fetcher"
version := "1.0"
scalaVersion := "2.10.4"

resolvers += "jitpack" at "https://jitpack.io"

val mongoDBDriverDep = "org.mongodb" %% "casbah" % "3.1.1"
val sparkCoreDep = "org.apache.spark" %% "spark-core" % "1.5.0" % "provided"
val sparkSQLDep = "org.apache.spark" %% "spark-sql" % "1.5.0" % "provided"
val sparkDataBricksDep = "com.databricks" %% "spark-xml" % "0.3.3"
val mongoDBHadoopCore = ("org.mongodb.mongo-hadoop" % "mongo-hadoop-core" % "1.5.1")
  .exclude("commons-logging", "commons-logging")
  .exclude("commons-beanutils", "commons-beanutils-core")
  .exclude("commons-collections", "commons-collections")

libraryDependencies ++= Seq(
  mongoDBDriverDep,
  sparkCoreDep,
  sparkSQLDep,
  sparkDataBricksDep,
  "com.github.WikiPlag" % "wikiplag_utils" % "-SNAPSHOT",
  "commons-cli" % "commons-cli" % "1.2",
  mongoDBHadoopCore
)

assemblyJarName in assembly := "wiki_data_fetcher.jar"
