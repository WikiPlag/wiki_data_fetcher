package de.htw.ai.wikiplag.spark

import com.mongodb.casbah.Imports._
import de.htw.ai.wikiplag.forwardreferencetable.ForwardReferenceTableImp
import de.htw.ai.wikiplag.parser.WikiDumpParser
import de.htw.ai.wikiplag.viewindex.ViewIndexBuilderImp
import org.apache.commons.cli.{DefaultParser, HelpFormatter, Option, OptionGroup, Options, ParseException}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by Max M on 11.06.2016.
  */
object SparkApp {

  //http://allegro.tech/2015/08/spark-kafka-integration.html
  class MongoDBClient(
                       createWikiCollection: () => MongoCollection,
                       createHashCollections: () => Map[Int, MongoCollection])
    extends Serializable {

    lazy val mongoCollection = createWikiCollection()
    lazy val collections = createHashCollections()

    def insertArticle(wikiID: Long,
                      title: String,
                      text: String,
                      viewIndex: List[(Int, Int, Int)]): Unit = {

      mongoCollection.insert(MongoDBObject(
        ("_id", wikiID),
        ("title", title),
        ("text", text),
        ("viewindex", viewIndex)
      ))
    }

    def insertNGramHashes(ngramSize: Int, wikiID: Long, hashes: Map[String, List[Int]]) = {
      collections.get(ngramSize).get.insert(MongoDBObject(
        ("_id", wikiID),
        ("hashes", hashes.map(x => {
          Map("hash" -> x._1, "occurs" -> x._2)
        }))
      ))
    }

  }

  object MongoDBClient {
    def apply(ngrams: List[Int]): MongoDBClient = {

      //http://stackoverflow.com/questions/25825058/why-multiple-mongodb-connecions-with-casbah
      val createWikiCollectionFct = () => {
        val mongoClient = MongoClient(
          new ServerAddress("hadoop03.f4.htw-berlin.de", 27020),
          List(MongoCredential.createCredential("REPLACE-ME", "REPLACE-ME", "REPLACE-ME".toCharArray))
        )

        sys.addShutdownHook {
          mongoClient.close()
        }
        mongoClient("s0546921")("wiki")
      }

      val createHashCollectionsFct = () => {
        val mongoClient = MongoClient(
          new ServerAddress("hadoop03.f4.htw-berlin.de", 27020),
          List(MongoCredential.createCredential("REPLACE-ME", "REPLACE-ME", "REPLACE-ME".toCharArray))
        )

        sys.addShutdownHook {
          mongoClient.close()
        }

        ngrams.map(x => {
          (x, mongoClient("s0546921")("wiki" + x))
        }).toMap
      }

      new MongoDBClient(createWikiCollectionFct, createHashCollectionsFct)
    }
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

    options.addOption(Option.builder("u")
      .longOpt("mongodb_user")
      .desc("MongoDB User")
      .required()
      .numberOfArgs(1)
      .`type`(classOf[String])
      .argName("user")
      .build())

    options.addOption(Option.builder("p")
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
      .longOpt("createindex")
      .desc("use db-entries to create an inverse index and stores it back")
      .numberOfArgs(0)
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
      val mongoDBUser = commandLine.getParsedOptionValue("user").asInstanceOf[String]
      val mongoDBPass = commandLine.getParsedOptionValue("password").asInstanceOf[String]

      if (commandLine.hasOption("e")) {
        extractAndSaveTokens(
          commandLine.getParsedOptionValue("hadoop_file").asInstanceOf[String],
          mongoDBPath,
          mongoDBUser,
          mongoDBPass)
        return
      }

      if (commandLine.hasOption("i")) {
        createInverseIndex(mongoDBPath, mongoDBUser, mongoDBPass)
        return
      }

      if (commandLine.hasOption("h")) {
        new HelpFormatter().printHelp("wiki_data_fetcher.jar", options)
      }

    } catch {
      case e: ParseException => {
        println("Unexpected exception: " + e.getMessage)
        e.printStackTrace()
      }
    }
  }

  def extractAndSaveTokens(hadoopFile: String, mongoDBPath: String, mongoDBUser: String, mongoDBPW: String) = {

    val ngrams = List(5, 7, 10)
    println(s"Start with File $hadoopFile with ngramSizes of: $ngrams ")

    val sparkConf = new SparkConf()
      .setAppName("WikiPlagSparkApp")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val mongoClient = sc.broadcast(MongoDBClient(ngrams))

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
        var isAtLeastOneHash = false

        for (n <- ngrams) {
          val frt = ForwardReferenceTableImp.buildForwardReferenceTable(tokens.map(_.toLowerCase()), n).toMap
          if (frt.nonEmpty) {
            mongoClient.value.insertNGramHashes(n, wikiID, frt)
            isAtLeastOneHash = true
          }
        }

        if (isAtLeastOneHash) {
          val viewIdx = ViewIndexBuilderImp.buildViewIndex(frontText, tokens)
          mongoClient.value.insertArticle(wikiID, t.getString(1), frontText, viewIdx)
        }

      })
    sc.stop()
  }

  def createInverseIndex(mongoDBPath: String, mongoDBUser: String, mongoDBPW: String) = {

  }

}
