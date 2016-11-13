package de.htw.ai.wikiplag.spark

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.{MongoClient, MongoClientOptions, MongoCollection}
import com.mongodb.ServerAddress


//http://allegro.tech/2015/08/spark-kafka-integration.html
class MongoDbClient(createWikiCollection: () => MongoCollection,
                    createInvIdxCollection: () => MongoCollection,
                    createHashCollections: () => Map[Int, MongoCollection])
  extends Serializable {

  lazy val wikiCollection = createWikiCollection()
  lazy val nGramCollections = createHashCollections()
  lazy val invIdxCollection = createInvIdxCollection()

  def insertArticle(wikiID: Long,
                    title: String,
                    text: String,
                    viewIndex: List[(Int, Int, Int)]): Unit = {

    wikiCollection.insert(MongoDBObject(
      ("_id", wikiID),
      ("title", title),
      ("text", text),
      ("viewindex", viewIndex)
    ))
  }

  def insertInverseIndex(word: String, doclist: List[(Long, List[Int])]) = {
    invIdxCollection.insert(MongoDBObject(
      ("_id", word),
      ("doclist", doclist)
    ))
  }

  def insertNGramHashes(ngramSize: Int, wikiID: Long, hashes: Map[String, List[Int]]) = {
    nGramCollections(ngramSize).insert(MongoDBObject(
      ("_id", wikiID),
      ("hashes", hashes.map(x => {
        Map("hash" -> x._1, "occurs" -> x._2)
      }))
    ))
  }
}

object MongoDBClient {

  val SERVER_PORT = 27020
  val ServerAddress = "hadoop03.f4.htw-berlin.de"
  val Password = "REPLACE-ME"
  val Database = "REPLACE-ME"
  val Username = "REPLACE-ME"
  val WikiCollectionName = "documents"
  val WikiInverseIndexCollectionName = "inv_idx"
  val WikiNGramCollectionPostfix = "-gram"

  def apply(ngrams: List[Int]): MongoDbClient = {

    //http://stackoverflow.com/questions/25825058/why-multiple-mongodb-connecions-with-casbah
    val createWikiCollectionFct = () => {
      val mongoClient = MongoClient(
        new ServerAddress(ServerAddress, SERVER_PORT),
        List(MongoCredential.createCredential(Username, Database, Password.toCharArray))
      )

      sys.addShutdownHook {
        mongoClient.close()
      }
      mongoClient(Database)(WikiCollectionName)
    }

    val createNGramCollectionsFct = () => {
      val mongoClient = MongoClient(
        new ServerAddress(ServerAddress, SERVER_PORT),
        List(MongoCredential.createCredential(Username, Database, Password.toCharArray))
      )

      sys.addShutdownHook {
        mongoClient.close()
      }

      ngrams.map(x => {
        (x, mongoClient(Database)("" + x + WikiNGramCollectionPostfix))
      }).toMap
    }

    val createInvIdxCollectionFct = () => {
      val mongoClient = MongoClient(
        new ServerAddress(ServerAddress, SERVER_PORT),
        List(MongoCredential.createCredential(Username, Database, Password.toCharArray))
      )

      sys.addShutdownHook {
        mongoClient.close()
      }
      mongoClient(Database)(WikiInverseIndexCollectionName)
    }

    new MongoDbClient(createWikiCollectionFct, createInvIdxCollectionFct, createNGramCollectionsFct)
  }
}