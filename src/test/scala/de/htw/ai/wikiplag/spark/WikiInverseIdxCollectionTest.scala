package de.htw.ai.wikiplag.spark

import com.mongodb.casbah.Imports._
import org.apache.commons.configuration2.builder.fluent.Configurations
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by Max M on 26.12.2016.
  */
@RunWith(classOf[JUnitRunner])
class WikiInverseIdxCollectionTest extends FunSuite with BeforeAndAfterAll {
  var mongoCollection: MongoCollection = _
  var idxColl: WikiInverseIdxCollection = _

  override protected def beforeAll(): Unit = {
    val config = new Configurations().properties("mongo.properties")

    val host = config.getString("database.host")
    val port = config.getInt("database.port")
    val username = config.getString("database.user")
    val password = config.getString("database.password")
    val database = config.getString("database.database")
    val testCollection = config.getString("database.collection")

    mongoCollection = MongoClient(new ServerAddress(host, port), List(MongoCredential.createCredential(username, database, password.toCharArray)))(database)(testCollection)
    mongoCollection.dropCollection()

    idxColl = WikiInverseIdxCollection(host, port, username, password, database, testCollection)
  }

  override protected def afterAll(): Unit = {
    mongoCollection.dropCollection()
  }

  /*
   * Tests
   */

  test("checkInsertingFormatAndTypes") {
    idxColl.upsertInverseIndex("MenschMeier", 123400, List(1, 2, 3, 4))

    val entry = mongoCollection.findOneByID("MenschMeier")
    assert(entry.isDefined)
    assert(entry.get.containsField("_id"))
    assert(entry.get.containsField("doc_list"))

    val doc_listField = entry.get.get("doc_list").asInstanceOf[BasicDBList]
    assert(doc_listField.size() == 1)

    val firstWikiEntry = doc_listField.get(0).asInstanceOf[BasicDBList]
    assert(firstWikiEntry.get(0).asInstanceOf[Long] == 123400)
    assert(firstWikiEntry.get(1).asInstanceOf[BasicDBList].size() == 4)
  }

  test("upsertWithDiffententKeys") {
    idxColl.upsertInverseIndex("Gleichung", 123401, List())
    idxColl.upsertInverseIndex("Artikel", 123402, List.range(0, 2000, 1))

    val query = "_id" $in List("Gleichung", "Artikel")
    val documentCount = mongoCollection.find(query).count()
    assert(2 == documentCount, "inserting 2 article with diffenrent '_id's should result in 2 entries")

    // test 'Gleichung'
    var entry = mongoCollection.findOneByID("Gleichung")
    assert(entry.isDefined)
    assert(entry.get.containsField("_id"))
    assert(entry.get.containsField("doc_list"))

    var doc_listField = entry.get.get("doc_list").asInstanceOf[BasicDBList]
    assert(doc_listField.size() == 1)

    var firstWikiEntry = doc_listField.get(0).asInstanceOf[BasicDBList]
    assert(firstWikiEntry.get(0).asInstanceOf[Long] == 123401)
    assert(firstWikiEntry.get(1).asInstanceOf[BasicDBList].size() == 0)

    // test 'Artikel'
    entry = mongoCollection.findOneByID("Artikel")
    assert(entry.isDefined)
    assert(entry.get.containsField("_id"))
    assert(entry.get.containsField("doc_list"))

    doc_listField = entry.get.get("doc_list").asInstanceOf[BasicDBList]
    assert(doc_listField.size() == 1)

    firstWikiEntry = doc_listField.get(0).asInstanceOf[BasicDBList]
    assert(firstWikiEntry.get(0).asInstanceOf[Long] == 123402)
    assert(firstWikiEntry.get(1).asInstanceOf[BasicDBList].size() == 2000)
  }

  test("upsertWithSameKey") {
    idxColl.upsertInverseIndex("Testwort", 123403, List(1, 2, 3, 4))
    idxColl.upsertInverseIndex("Testwort", 123404, List(1, 2, 3))

    val query = MongoDBObject("_id" -> "Testwort")
    assert(1 == mongoCollection.find(query).count(), "inserting 2 article with the same '_id' should result in a merged doc_list entries")

    var entry = mongoCollection.findOneByID("Testwort")
    assert(entry.isDefined)
    assert(entry.get.containsField("_id"))
    assert(entry.get.containsField("doc_list"))

    var doc_listField = entry.get.get("doc_list").asInstanceOf[BasicDBList]
    assert(doc_listField.size() == 2)

    var firstWikiEntry = doc_listField.get(0).asInstanceOf[BasicDBList]
    assert(firstWikiEntry.get(0).asInstanceOf[Long] == 123403)
    assert(firstWikiEntry.get(1).asInstanceOf[BasicDBList].size() == 4)

    var secondWikiEntry = doc_listField.get(1).asInstanceOf[BasicDBList]
    assert(secondWikiEntry.get(0).asInstanceOf[Long] == 123404)
    assert(secondWikiEntry.get(1).asInstanceOf[BasicDBList].size() == 3)

    // a third entry

    idxColl.upsertInverseIndex("Testwort", 123405, List(10, 11))

    entry = mongoCollection.findOneByID("Testwort")
    assert(entry.isDefined)
    assert(entry.get.containsField("_id"))
    assert(entry.get.containsField("doc_list"))

    doc_listField = entry.get.get("doc_list").asInstanceOf[BasicDBList]
    assert(doc_listField.size() == 3)

    firstWikiEntry = doc_listField.get(0).asInstanceOf[BasicDBList]
    assert(firstWikiEntry.get(0).asInstanceOf[Long] == 123403)
    assert(firstWikiEntry.get(1).asInstanceOf[BasicDBList].size() == 4)

    secondWikiEntry = doc_listField.get(1).asInstanceOf[BasicDBList]
    assert(secondWikiEntry.get(0).asInstanceOf[Long] == 123404)
    assert(secondWikiEntry.get(1).asInstanceOf[BasicDBList].size() == 3)

    secondWikiEntry = doc_listField.get(2).asInstanceOf[BasicDBList]
    assert(secondWikiEntry.get(0).asInstanceOf[Long] == 123405)
    assert(secondWikiEntry.get(1).asInstanceOf[BasicDBList].size() == 2)

  }

}