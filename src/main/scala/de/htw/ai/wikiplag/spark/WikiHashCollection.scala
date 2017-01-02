package de.htw.ai.wikiplag.spark

import com.mongodb.casbah.Imports._

/**
  * Created by maikt on 30.11.2016.
  */
class WikiHashCollection(createHashCollection: () => MongoCollection) extends Serializable {
  private lazy val hashCollection = createHashCollection()

  def insertNGramHashes(ngramSize: Int, wikiID: Long, hashes: Map[String, List[Int]]): Unit = {
    hashCollection.insert(MongoDBObject(
      ("_id", wikiID),
      ("hashes", hashes.map(x => {
        Map("hash" -> x._1, "occurs" -> x._2)
      }))
    ))
  }
}

object WikiHashCollection {
  val WikiNGramCollectionPrefix = "hash"
  val WikiNGramCollectionPostfix = "-gram"

  def apply(ngramSize: Int, mongoDBPath: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String, mongoDBDatabase: String): WikiHashCollection = {

    val createHashCollection = () => {
      val mongoClient = MongoClient(
        new ServerAddress(mongoDBPath, mongoDBPort),
        List(MongoCredential.createCredential(mongoDBUser, mongoDBDatabase, mongoDBPW.toCharArray))
      )

      sys.addShutdownHook {
        mongoClient.close()
      }
      mongoClient(mongoDBDatabase)(WikiNGramCollectionPrefix + ngramSize + WikiNGramCollectionPostfix)
    }

    new WikiHashCollection(createHashCollection)
  }

}
