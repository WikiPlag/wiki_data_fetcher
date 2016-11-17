package de.htw.ai.wikiplag.spark

import de.htw.ai.wikiplag.forwardreferencetable.ForwardReferenceTableImp
import de.htw.ai.wikiplag.parser.WikiDumpParser
import de.htw.ai.wikiplag.viewindex.ViewIndexBuilderImp
import org.apache.commons.cli.{DefaultParser, HelpFormatter, Option, OptionGroup, Options}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by Max M on 11.06.2016.
  */
object SparkApp {

  def printHelp(options: Options) = {
    val header = "\nOptions:"
    val footer = "\nProjektstudium Wikiplag\nHTW Berlin\n"
    new HelpFormatter().printHelp(110, "wiki_data_fetcher.jar", header, options, footer, true)
  }

  def createCLIOptions() = {
    val options = new Options()
    options.addOption(Option.builder("h")
      .longOpt("help")
      .hasArg(false)
      .build())

    /* MongoDB Settings */

    options.addOption(Option.builder("m")
      .longOpt("mongodb_path")
      .desc("MongoDB Path")
      .required()
      .numberOfArgs(1)
      .`type`(classOf[String])
      .argName("path")
      .build())

    options.addOption(Option.builder("p")
      .longOpt("mongodb_port")
      .desc("MongoDB Port")
      .required()
      .numberOfArgs(1)
      .`type`(classOf[String])
      .argName("port")
      .build())

    options.addOption(Option.builder("u")
      .longOpt("mongodb_user")
      .desc("MongoDB User")
      .required()
      .numberOfArgs(1)
      .`type`(classOf[String])
      .argName("user")
      .build())

    options.addOption(Option.builder("pw")
      .longOpt("mongodb_password")
      .desc("MongoDB Password")
      .required()
      .numberOfArgs(1)
      .`type`(classOf[String])
      .argName("password")
      .build())

    /* Commands */

    val group = new OptionGroup()

    group.addOption(Option.builder("e")
      .longOpt("extract")
      .desc("parse wiki XML file and saves in a db")
      .numberOfArgs(1)
      .argName("hadoop_file")
      .`type`(classOf[String])
      .build()
    )

    group.addOption(Option.builder("i")
      .longOpt("index")
      .desc("use db-entries to create an inverse index and stores it back")
      .numberOfArgs(0)
      .build()
    )

    group.addOption(Option.builder("n")
      .longOpt("ngrams")
      .desc("use db-entries to create hashed n-grams of a given size")
      .numberOfArgs(1)
      .`type`(classOf[Int])
      .argName("ngram")
      .build()
    )

    options.addOptionGroup(group)
    options
  }

  def main(args: Array[String]) {
    val options = createCLIOptions()

    try {
      val commandLine = new DefaultParser().parse(options, args)
      val mongoDBPath = commandLine.getParsedOptionValue("path").asInstanceOf[String]
      val mongoDBPort = commandLine.getParsedOptionValue("port").asInstanceOf[Int]
      val mongoDBUser = commandLine.getParsedOptionValue("user").asInstanceOf[String]
      val mongoDBPass = commandLine.getParsedOptionValue("password").asInstanceOf[String]

      if (commandLine.hasOption("h")) {
        printHelp(options)
      }

      if (commandLine.hasOption("e")) {
        val file = commandLine.getParsedOptionValue("hadoop_file").asInstanceOf[String]
        extractText(file, mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPass)

      } else if (commandLine.hasOption("i")) {
        createInverseIndex(mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPass)

      } else if (commandLine.hasOption("n")) {
        val ngramSize = commandLine.getParsedOptionValue("ngram").asInstanceOf[Int]
        buildNGrams(ngramSize, mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPass)
      }

    } catch {
      case e: Exception =>
        println("Unexpected exception: " + e.getMessage)
        printHelp(options)
    }
  }

  /*
   * core functions
   */

  def extractText(hadoopFile: String, mongoDBPath: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String) = {
    val sparkConf = new SparkConf().setAppName("WikiPlagSparkApp")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.load("com.databricks.spark.xml", Map("path" -> hadoopFile, "rowTag" -> "page"))

    val wikiClient = sc.broadcast(WikiCollection(mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPW, "wiki"))

    df
      .filter("ns = 0")
      .select("id", "title", "revision.text")
      .foreach(t => {
        val wikiID = t.getLong(0)
        val rawText = t.getStruct(2).getString(0)
        val frontText = WikiDumpParser.parseXMLWikiPage(rawText)
        val tokens = WikiDumpParser.extractWikiDisplayText(frontText)

        val viewIdx = ViewIndexBuilderImp.buildViewIndex(frontText, tokens)
        wikiClient.value.insertArticle(wikiID, t.getString(1), frontText, viewIdx)
      })
    sc.stop()
  }

  def buildNGrams(ngramSize: Int, mongoDBPath: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String) = {
    println(s"Generate N-Grams of size: $ngramSize")

    val sparkConf = new SparkConf().setAppName("WikiPlagSparkApp")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val mongoClient = sc.broadcast(MongoDbClient(ngrams))

    val df = sqlContext
      .load("com.databricks.spark.xml", Map("path" -> hadoopFile, "rowTag" -> "page"))

    df
      .filter("ns = 0")
      .select("id", "title", "revision.text")
      .foreach(t => {
        val wikiID = t.getLong(0)
        val rawText = t.getStruct(2).getString(0)
        val frontText = WikiDumpParser.parseXMLWikiPage(rawText)
        val tokens = WikiDumpParser.extractWikiDisplayText(frontText)

        for (n <- ngrams) {
          val frt = ForwardReferenceTableImp.buildForwardReferenceTable(tokens.map(_.toLowerCase()), n).toMap
          if (frt.nonEmpty) {
            mongoClient.value.insertNGramHashes(n, wikiID, frt)
          }
        }
      })
    sc.stop()
  }

  def createInverseIndex(mongoDBPath: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String) = {

  }

}
