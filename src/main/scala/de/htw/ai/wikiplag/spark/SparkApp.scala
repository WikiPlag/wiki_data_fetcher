package de.htw.ai.wikiplag.spark

import de.htw.ai.wikiplag.data.InverseIndexBuilderImpl
import de.htw.ai.wikiplag.forwardreferencetable.ForwardReferenceTableImp
import de.htw.ai.wikiplag.parser.WikiDumpParser
import de.htw.ai.wikiplag.viewindex.ViewIndexBuilderImp
import org.apache.commons.cli._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by Max M on 11.06.2016.
  */
object SparkApp {
  private def printHelp(options: Options) = {
    val header = "\nOptions:"
    val footer = "\nProjektstudium Wikiplag\nHTW Berlin\n"
    new HelpFormatter().printHelp(110, "wiki_data_fetcher.jar", header, options, footer, true)
  }

  private def createCLIOptions() = {
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

    OptionBuilder.withLongOpt("mongodb_pass")
    OptionBuilder.withDescription("MongoDB Password")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("pass")
    options.addOption(OptionBuilder.create("pw"))

    /* Commands */

    val group = new OptionGroup()
    group.setRequired(true)

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
      val mongoDBPass = commandLine.getParsedOptionValue("mongodb_pass").asInstanceOf[String]

      if (commandLine.hasOption("h")) {
        printHelp(options)
        return
      }

      if (commandLine.hasOption("e")) {
        val file = commandLine.getParsedOptionValue("e").asInstanceOf[String]
        extractText(file, mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPass)

      } else if (commandLine.hasOption("i")) {
        createInverseIndex(mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPass)

      } else if (commandLine.hasOption("n")) {
        val ngramSize = commandLine.getParsedOptionValue("n").asInstanceOf[Int]
        buildNGrams(ngramSize, mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPass)
      }

    } catch {
      case e: ParseException =>
        println("Unexpected exception: " + e.getMessage)
        printHelp(options)
      case e: Exception =>
        e.printStackTrace()
        printHelp(options)
    }
  }

  /*
   * core functions
   */

  private def extractText(hadoopFile: String, mongoDBPath: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String) = {
    println("hadoopfile: " + hadoopFile)
    val sparkConf = new SparkConf().setAppName("WikiPlagSparkApp")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "page")
      .load(hadoopFile)

    val wikiClient = sc.broadcast(WikiDocumentCollection(mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPW, "wikiplag"))

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

  private def buildNGrams(ngramSize: Int, mongoDBPath: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String) = {
    println(s"Generate N-Grams of size: $ngramSize")
//    val ngrams = List(5, 7, 10)
//    val sparkConf = new SparkConf().setAppName("WikiPlagSparkApp")
//
//    val sc = new SparkContext(sparkConf)
//    val sqlContext = new SQLContext(sc)
//    val mongoClient = sc.broadcast(MongoDbClient(ngrams))
//
//    val df = sqlContext
//      .load("com.databricks.spark.xml", Map("path" -> hadoopFile, "rowTag" -> "page"))
//
//    df
//      .filter("ns = 0")
//      .select("id", "title", "revision.text")
//      .foreach(t => {
//        val wikiID = t.getLong(0)
//        val rawText = t.getStruct(2).getString(0)
//        val frontText = WikiDumpParser.parseXMLWikiPage(rawText)
//        val tokens = WikiDumpParser.extractWikiDisplayText(frontText)
//
//        for (n <- ngrams) {
//          val frt = ForwardReferenceTableImp.buildForwardReferenceTable(tokens.map(_.toLowerCase()), n).toMap
//          if (frt.nonEmpty) {
//            mongoClient.value.insertNGramHashes(n, wikiID, frt)
//          }
//        }
//      })
//    sc.stop()
  }

  private def createInverseIndex(mongoDBPath: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String) = {
    println("createInverseIndex")
    val sparkConf = new SparkConf().setAppName("WikiPlagSparkApp")

    val sc = new SparkContext(sparkConf)
    val uri = "mongodb://" + mongoDBPath + ":" + mongoDBPort + "/wikiplag.documents"
    val authUri = "mongodb://" + mongoDBUser + ":" + mongoDBPW + "@" + mongoDBPath + ":" + mongoDBPort + "/wikiplag"
    // set up parameters for reading from MongoDB via Hadoop input format
    val config = new Configuration()
    config.set("mongo.input.uri", uri)
    config.set("mongo.auth.uri", authUri)

    // read the 1-minute bars from MongoDB into Spark RDD format
    val casRdd = sc.newAPIHadoopRDD(config,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[org.bson.BSONObject])

    val documents = casRdd.map(x => (x._2.get("_id").asInstanceOf[Long], x._2.get("title").toString, x._2.get("text").toString))
    val idTokens = documents.map(x => (x._1, InverseIndexBuilderImpl.buildIndexKeys(x._3)))
    val invIndexEntries = idTokens.map(x => InverseIndexBuilderImpl.buildInverseIndexEntry(x._1, x._2))
    val idxColl = sc.broadcast(WikiInverseIdxCollection(mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPW, "wikiplag"))

    println(invIndexEntries.take(1).toList.toString())

    InverseIndexBuilderImpl.mergeInverseIndexEntries(invIndexEntries.toLocalIterator.toList)
      .foreach(x => {
        idxColl.value.insertInverseIndex(x._1, x._2)
      })

  }

  private def createInverseIndexCasbah(mongoDBPath: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String) = {
    println("createInverseIndex")
    val sparkConf = new SparkConf().setAppName("WikiPlagSparkApp")

    val sc = new SparkContext(sparkConf)
    val documentColl = sc.broadcast(WikiDocumentCollection(mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPW, "wikiplag"))
    val idxColl = WikiInverseIdxCollection(mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPW, "wikiplag")

    val r = sc
      .parallelize(documentColl.value.iterator().limit(100).toIndexedSeq) // ggf. limit(x)
      .map(x => {
      val text = x.get("text").asInstanceOf[String]
      if (text != null || text.nonEmpty) {
        val tokens = InverseIndexBuilderImpl.buildIndexKeys(text)
        if (tokens != null || tokens.nonEmpty) {
          val id = x.get("_id").asInstanceOf[Long].toInt
          val idx = InverseIndexBuilderImpl.buildInverseIndexEntry(id, tokens)
          idx
        } else {
          null
        }
      } else {
        null
      }
    })
      .collect()
      .filter(x => x != null)
      .toList

    InverseIndexBuilderImpl.mergeInverseIndexEntries(r)
      .foreach(x => {
        idxColl.insertInverseIndex(x._1, x._2)
      })
  }

  private def createInverseIndexCasbah2(mongoDBPath: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String) = {
    println("createInverseIndex 2, insert per document")
    val sparkConf = new SparkConf().setAppName("WikiPlagSparkApp")

    val sc = new SparkContext(sparkConf)
    val documentColl = sc.broadcast(WikiDocumentCollection(mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPW, "wikiplag"))
    val idxColl = sc.broadcast(WikiInverseIdxCollection(mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPW, "wikiplag"))

    val r = sc
      .parallelize(documentColl.value.iterator().toIndexedSeq)
      .foreach(x => {
        val text = x.get("text").asInstanceOf[String]
        if (text != null || text.nonEmpty) {
          val tokens = InverseIndexBuilderImpl.buildIndexKeys(text)
          if (tokens != null || tokens.nonEmpty) {
            val id = x.get("_id").asInstanceOf[Long].toInt
            val idx = InverseIndexBuilderImpl.buildInverseIndexEntry(id, tokens)
            idx.foreach(x => {
              idxColl.value.upsertInverseIndex(x._1, id, x._2._2)
            })
          }
        }
      })
  }

}
