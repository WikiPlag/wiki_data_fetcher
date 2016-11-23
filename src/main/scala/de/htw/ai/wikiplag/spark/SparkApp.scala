package de.htw.ai.wikiplag.spark

import de.htw.ai.wikiplag.forwardreferencetable.ForwardReferenceTableImp
import de.htw.ai.wikiplag.parser.WikiDumpParser
import de.htw.ai.wikiplag.viewindex.ViewIndexBuilderImp
import org.apache.commons.cli._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

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
    OptionBuilder.withLongOpt("help")
    OptionBuilder.hasArg(false)
    options.addOption(OptionBuilder.create("h"))

    /* MongoDB Settings */

    OptionBuilder.withLongOpt("mongodb_path")
    OptionBuilder.withDescription("MongoDB Path")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("path")
    options.addOption(OptionBuilder.create("m"))

    OptionBuilder.withLongOpt("mongodb_port")
    OptionBuilder.withDescription("MongoDB Port")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[Number])
    OptionBuilder.withArgName("port")
    options.addOption(OptionBuilder.create("p"))

    OptionBuilder.withLongOpt("mongodb_user")
    OptionBuilder.withDescription("MongoDB user")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("user")
    options.addOption(OptionBuilder.create("u"))

    OptionBuilder.withLongOpt("mongodb_password")
    OptionBuilder.withDescription("MongoDB Password")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("password")
    options.addOption(OptionBuilder.create("pw"))

    /* Commands */

    val group = new OptionGroup()

    OptionBuilder.withLongOpt("extract")
    OptionBuilder.withDescription("parse wiki XML file and saves in a db")
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("hadoop_file")
    group.addOption(OptionBuilder.create("e"))

    OptionBuilder.withLongOpt("index")
    OptionBuilder.withDescription("use db-entries to create an inverse index and stores it back")
    OptionBuilder.hasArgs(0)
    group.addOption(OptionBuilder.create("i"))

    OptionBuilder.withLongOpt("ngrams")
    OptionBuilder.withDescription("use db-entries to create hashed n-grams of a given size")
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[Number])
    OptionBuilder.withArgName("ngram")
    group.addOption(OptionBuilder.create("n"))

    options.addOptionGroup(group)
    options
  }

  def main(args: Array[String]) {
    val options = createCLIOptions()

    try {
      val commandLine = new GnuParser().parse(options, args)
      val mongoDBPath = commandLine.getParsedOptionValue("mongodb_path").asInstanceOf[String]
      val mongoDBPort = commandLine.getParsedOptionValue("mongodb_port").asInstanceOf[Number].intValue()
      val mongoDBUser = commandLine.getParsedOptionValue("mongodb_user").asInstanceOf[String]
      val mongoDBPass = commandLine.getParsedOptionValue("mongodb_password").asInstanceOf[String]

      if (commandLine.hasOption("h")) {
        printHelp(options)
      }

      if (commandLine.hasOption("e")) {
        val file = commandLine.getParsedOptionValue("e").asInstanceOf[String]
        extractText(file, mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPass)

      } else if (commandLine.hasOption("i")) {
        createInverseIndex(mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPass)

      } else if (commandLine.hasOption("n")) {
        val ngramSize = commandLine.getParsedOptionValue("ngram").asInstanceOf[Int]
        val file = commandLine.getParsedOptionValue("hadoop_file").asInstanceOf[String]
        buildNGrams(ngramSize, file, mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPass)
      }

    } catch {
      case e: Exception =>
        println("Unexpected exception: ")
        e.printStackTrace()
        printHelp(options)
    }
  }

  /*
   * core functions
   */

  def extractText(hadoopFile: String, mongoDBPath: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String) = {
    println("hadoopfile: " + hadoopFile)
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

  def buildNGrams(ngramSize: Int, hadoopFile: String, mongoDBPath: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String) = {
    println(s"Generate N-Grams of size: $ngramSize")
    val ngrams = List(5, 7, 10)
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
