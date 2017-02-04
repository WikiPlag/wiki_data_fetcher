package de.htw.ai.wikiplag.spark

import java.util.Locale

import com.mongodb.{BasicDBList, BasicDBObject}
import com.mongodb.casbah.Imports.$addToSet
import com.mongodb.hadoop.MongoOutputFormat
import com.mongodb.hadoop.io.MongoUpdateWritable
import de.htw.ai.wikiplag.data.InverseIndexBuilderImpl
import de.htw.ai.wikiplag.parser.WikiDumpParser
import de.htw.ai.wikiplag.viewindex.ViewIndexBuilderImp
import org.apache.commons.cli._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.{BsonArray, BsonInt32, BsonInt64}

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

    OptionBuilder.withLongOpt("mongodb_host")
    OptionBuilder.withDescription("MongoDB Host")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("host")
    options.addOption(OptionBuilder.create("mh"))

    OptionBuilder.withLongOpt("mongodb_port")
    OptionBuilder.withDescription("MongoDB Port")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[Number])
    OptionBuilder.withArgName("port")
    options.addOption(OptionBuilder.create("mp"))

    OptionBuilder.withLongOpt("mongodb_user")
    OptionBuilder.withDescription("MongoDB User")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("user")
    options.addOption(OptionBuilder.create("mu"))

    OptionBuilder.withLongOpt("mongodb_password")
    OptionBuilder.withDescription("MongoDB Password")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("password")
    options.addOption(OptionBuilder.create("mpw"))

    OptionBuilder.withLongOpt("mongodb_database")
    OptionBuilder.withDescription("MongoDB Database")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("database")
    options.addOption(OptionBuilder.create("md"))

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
      val mongoDBPath = commandLine.getParsedOptionValue("mongodb_host").asInstanceOf[String]
      val mongoDBPort = commandLine.getParsedOptionValue("mongodb_port").asInstanceOf[Number].intValue()
      val mongoDBUser = commandLine.getParsedOptionValue("mongodb_user").asInstanceOf[String]
      val mongoDBPass = commandLine.getParsedOptionValue("mongodb_password").asInstanceOf[String]
      val mongoDBDatabase = commandLine.getParsedOptionValue("mongodb_database").asInstanceOf[String]

      if (commandLine.hasOption("h")) {
        printHelp(options)
        return
      }

      if (commandLine.hasOption("e")) {
        val file = commandLine.getParsedOptionValue("e").asInstanceOf[String]
        extractText(file, mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPass, mongoDBDatabase)

      } else if (commandLine.hasOption("i")) {
        createInverseIndex(mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPass, mongoDBDatabase)

      } else if (commandLine.hasOption("n")) {
        val ngramSize = commandLine.getParsedOptionValue("n").asInstanceOf[Int]
        buildNGrams(ngramSize, mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPass, mongoDBDatabase)
      }

    } catch {
      case e: ParseException =>
        println("Unexpected ParseException: " + e.getMessage)
        printHelp(options)
      case e: Exception =>
        e.printStackTrace()
        printHelp(options)
    }
  }

  /*
   * core functions
   */

  private def extractText(hadoopFile: String, mongoDBPath: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String, mongoDBDatabase: String) = {
    println("hadoopfile: " + hadoopFile)
    val sparkConf = new SparkConf().setAppName("WikiPlagSparkApp")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "page")
      .load(hadoopFile)

    val wikiClient = sc.broadcast(WikiDocumentCollection(mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPW, mongoDBDatabase))

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

  private def buildNGrams(ngramSize: Int, mongoDBPath: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String, mongoDBDatabase: String) = {
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

  private def createInverseIndex(mongoDBPath: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String, mongoDBDatabase: String) = {
    println("createInverseIndex")
    val sparkConf = new SparkConf().setAppName("WikiPlagSparkApp")

    val sc = new SparkContext(sparkConf)
    val uri = "mongodb://" + mongoDBPath + ":" + mongoDBPort + "/wikiplag."
    val authUri = "mongodb://" + mongoDBUser + ":" + mongoDBPW + "@" + mongoDBPath + ":" + mongoDBPort + "/wikiplag"
    // set up parameters for reading from MongoDB via Hadoop input format
    val inputConfig = new Configuration()
    inputConfig.set("mongo.input.uri", uri + "documents")
    inputConfig.set("mongo.auth.uri", authUri)

    val inputRdd = sc.newAPIHadoopRDD(inputConfig,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[org.bson.BSONObject])

    val documents = inputRdd.mapValues(x => x.get("text").toString)
    val idTokens = documents.mapValues(x => InverseIndexBuilderImpl.buildIndexKeys(x))
    val invIndexEntries = idTokens.map(x => InverseIndexBuilderImpl.buildInverseIndexEntry(x._1.asInstanceOf[Long], x._2))
    val idxColl = sc.broadcast(WikiInverseIdxCollection(mongoDBPath, mongoDBPort, mongoDBUser, mongoDBPW, mongoDBDatabase))

    val updates = invIndexEntries.flatMap(
      x => x.map(y => (y._1, {
        val entries = new BasicDBList()
        y._2._2.foreach(x => entries.add(Int.box(x)))

        val docList = new BasicDBList()
        docList.add(Long.box(y._2._1))
        docList.add(entries)

        new MongoUpdateWritable(new BasicDBObject("_id", y._1.toLowerCase(Locale.ROOT)), // Query
          new BasicDBObject("$addToSet", new BasicDBObject("doc_list", docList)), // Update operation
          true, // Upsert
          false // Update multiple documents
        )
      })
      )
    )

    //invIndexEntries.foreach(x => {
    //  x.foreach(y => {
    //    idxColl.value.upsertInverseIndex(y._1, y._2._1, y._2._2)
    //  })
    //})

    // Create a separate Configuration for saving data back to MongoDB.
    val outputConfig = new Configuration()
    outputConfig.set("mongo.output.uri", uri + "inv_idx_3")
    outputConfig.set("mongo.auth.uri", authUri)

    // Now we call saveAsNewAPIHadoopFile, using MongoUpdateWritable as the
    // value class.
    updates.saveAsNewAPIHadoopFile(
      "file:///this-is-completely-unused",
      classOf[Object],
      classOf[MongoUpdateWritable],
      classOf[MongoOutputFormat[Object, MongoUpdateWritable]],
      outputConfig)
  }

}
