package de.htw.ai.wikiplag.spark

import com.mongodb.casbah.Imports._

//http://allegro.tech/2015/08/spark-kafka-integration.html
//http://stackoverflow.com/questions/25825058/why-multiple-mongodb-connecions-with-casbah
class WikiDocumentCollection(createWikiCollection: () => MongoCollection) extends Serializable {
  private lazy val wikiCollection = createWikiCollection()

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

  def iterator(): MongoCursor = {
    wikiCollection.find()
  }
}

object WikiDocumentCollection {
  val WikiCollectionName = "documents"

  def apply(mongoDBPath: String, port: Int, mongoDBUser: String, mongoDBPW: String, database: String): WikiDocumentCollection = {

    val createWikiCollectionFct = () => {
      val mongoClient = MongoClient(
        new ServerAddress(mongoDBPath, port),
        List(MongoCredential.createCredential(mongoDBUser, mongoDBUser, mongoDBPW.toCharArray))
      )
      sys.addShutdownHook {
        mongoClient.close()
      }
      mongoClient(database)(WikiCollectionName)
    }

    new WikiDocumentCollection(createWikiCollectionFct)
  }
}