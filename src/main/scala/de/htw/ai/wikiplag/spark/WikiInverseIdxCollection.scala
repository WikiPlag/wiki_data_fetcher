package de.htw.ai.wikiplag.spark

import com.mongodb.casbah.Imports._

class WikiInverseIdxCollection(createInvIdxCollection: () => MongoCollection) extends Serializable {
  private lazy val invIdxCollection = createInvIdxCollection()

  /**
    * insert a new token with all occurrences
    *
    * @param word    Token, e.g. house, wikipedia, ...
    * @param doclist List of [(wiki_id, List(occurrences))]
    */
  def insertInverseIndex(word: String, doclist: List[(Int, List[Int])]): Unit = {
    invIdxCollection.insert(MongoDBObject(
      ("_id", word),
      ("doc_list", doclist)
    ))
  }

  /**
    * insert or update an entry (via id)
    *
    * @param word        Token, e.g. house, wikipedia, ...
    * @param wiki_id     wikipedia article id
    * @param occurrences List of [occurences]
    */
  def upsertInverseIndex(word: String, wiki_id: Int, occurrences: List[Int]): Unit = {
    val oldEntry = invIdxCollection.findOneByID(MongoDBObject("_id" -> word))
    if (oldEntry.isDefined) {
      val newEntry = MongoDBObject(
        ("_id", word),
        ("doc_list", List((wiki_id, occurrences)))
      )
      invIdxCollection.insert(newEntry)
    } else {
      val newEntry = $push("doclist" -> List((wiki_id, occurrences)))
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