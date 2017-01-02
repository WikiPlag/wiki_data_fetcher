package de.htw.ai.wikiplag.spark

import com.mongodb.DuplicateKeyException
import com.mongodb.casbah.Imports._
import org.apache.log4j.LogManager

class WikiInverseIdxCollection(createInvIdxCollection: () => MongoCollection) extends Serializable {
  private lazy val invIdxCollection = createInvIdxCollection()
  // use at least info-lvl, cause of our root htw-settings
  private lazy val log = LogManager.getRootLogger

  /**
    * insert a new token with all occurrences
    *
    * @param word    Token, e.g. house, wikipedia, ...
    * @param doclist List of [(wiki_id, List(occurrences))]
    */
  def insertInverseIndex(word: String, doclist: List[(Long, List[Int])]): Unit = {
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
  def upsertInverseIndex(word: String, wiki_id: Long, occurrences: List[Int]): Unit = {
    // db.inv_idx.find({ "_id": { $eq: "Ereignisse" }})
    val oldEntry = invIdxCollection.findOneByID(word)
    if (oldEntry.isEmpty) {
      log.info(s"insert $word ${oldEntry.isEmpty}")
      try {
        invIdxCollection.insert(MongoDBObject(
          ("_id", word),
          ("doc_list", List((wiki_id, occurrences)))
        ))
      } catch {
        case e: DuplicateKeyException =>
          log.warn(s"DuplicateKey $word")
          val entry = invIdxCollection.findOneByID(word)
          val newEntry = $push("doc_list" -> (wiki_id, occurrences))
          invIdxCollection.update(entry.get, newEntry, concern = WriteConcern.Safe)
        case e: Exception =>
          log.error("unknown error", e)
          throw e
      }
    } else {
      log.info(s"update $word ${oldEntry.isEmpty}")
      val newEntry = $push("doc_list" -> (wiki_id, occurrences))
      invIdxCollection.update(oldEntry.get, newEntry, concern = WriteConcern.Safe)
    }
  }
}

object WikiInverseIdxCollection {
  val WikiInverseIndexDefaultCollectionName = "inv_idx"

  def apply(mongoDBPath: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String, mongoDBDatabase: String, mongoCollectionName: String = WikiInverseIndexDefaultCollectionName): WikiInverseIdxCollection = {

    val createInvIdxCollectionFct = () => {
      val mongoClient = MongoClient(
        new ServerAddress(mongoDBPath, mongoDBPort),
        List(MongoCredential.createCredential(mongoDBUser, mongoDBDatabase, mongoDBPW.toCharArray)),
        MongoClientOptions(writeConcern = WriteConcern.Safe)
      )

      sys.addShutdownHook {
        mongoClient.close()
      }
      mongoClient(mongoDBDatabase)(mongoCollectionName)
    }

    new WikiInverseIdxCollection(createInvIdxCollectionFct)
  }
}