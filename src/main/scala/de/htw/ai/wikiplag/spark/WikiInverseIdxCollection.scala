package de.htw.ai.wikiplag.spark

import com.mongodb.casbah.Imports._

class WikiInverseIdxCollection(createInvIdxCollection: () => MongoCollection) extends Serializable {
  private lazy val invIdxCollection = createInvIdxCollection()

  /**
    * Insert a new token with all occurences
    *
    * @param word    Token, e.g. house, wikipedia, ...
    * @param doclist List of [(wiki_id, List(occurences))]
    */
  def insertInverseIndex(word: String, doclist: List[(Int, List[Int])]): Unit = {
    invIdxCollection.insert(MongoDBObject(
      ("_id", word),
      ("doclist", doclist)
    ))
  }

  def upsertInverseIndex(word: String, wiki_id: Int, occurrences: List[Int]): Unit = {
    val oldEntry = invIdxCollection.findOneByID(MongoDBObject("_id" -> word))
    if (oldEntry.isDefined) { // insert new
      val newEntry = MongoDBObject(
        ("_id", word),
        ("doclist", List((wiki_id, occurrences)))
      )
      invIdxCollection.insert(newEntry)
    } else { // update
//      val newEntry = MongoDBObject(
//          "$push" -> MongoDBObject("doclist" -> occurrences)
//        )
      val newEntry = $push("doclist" -> occurrences)
      invIdxCollection.update(oldEntry.get, newEntry)
    }
  }

}

object WikiInverseIdxCollection {
  val WikiInverseIndexCollectionName = "inv_idx"

  def apply(mongoDBPath: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String, mongoDBDatabase: String): WikiInverseIdxCollection = {

    val createInvIdxCollectionFct = () => {
      val mongoClient = MongoClient(
        new ServerAddress(mongoDBPath, mongoDBPort),
        List(MongoCredential.createCredential(mongoDBUser, mongoDBDatabase, mongoDBPW.toCharArray))
      )

      sys.addShutdownHook {
        mongoClient.close()
      }
      mongoClient(mongoDBDatabase)(WikiInverseIndexCollectionName)
    }

    new WikiInverseIdxCollection(createInvIdxCollectionFct)
  }
}