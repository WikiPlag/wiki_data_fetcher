// import AssemblyKeys._

// assemblySettings

name := "wiki_data_fetcher"
version := "1.0"
scalaVersion := "2.10.6"

resolvers += "jitpack" at "https://jitpack.io"

val mongoDBDriverDep = "org.mongodb" %% "casbah" % "3.1.1"
val sparkCoreDep = "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"
val sparkSQLDep = "org.apache.spark" %% "spark-sql" % "1.3.0" % "provided"

libraryDependencies ++= Seq(
  mongoDBDriverDep,
  sparkCoreDep,
  sparkSQLDep,
  "com.github.WikiPlag" % "wikiplag_utils" % "-SNAPSHOT",
  "commons-cli" % "commons-cli" % "1.3.1"
)

// assemblyJarName in assembly := "something.jar"