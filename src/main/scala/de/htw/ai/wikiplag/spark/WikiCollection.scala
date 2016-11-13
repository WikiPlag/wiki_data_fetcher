package de.htw.ai.wikiplag.spark

import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.{MongoClient, MongoCollection}

class WikiCollection(createWikiCollection: () => MongoCollection) {
  lazy val wikiCollection = createWikiCollection()

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
}

object WikiCollection {
  val WikiCollectionName = "documents"

  def apply(mongoDBPath: String, port: Int, mongoDBUser: String, mongoDBPW: String, database: String): WikiCollection = {
    //http://stackoverflow.com/questions/25825058/why-multiple-mongodb-connecions-with-casbah
    val createWikiCollectionFct = () => {

      val mongoClient = MongoClient(
        new ServerAddress(mongoDBPath, port),
        List(MongoCredential.createCredential(mongoDBUser, mongoDBUser, mongoDBPW.toCharArray))
      )

      mongoClient(database)(WikiCollectionName)

    }
    new WikiCollection(createWikiCollectionFct)
  }
}