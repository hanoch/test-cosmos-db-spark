package com.esri.cosmosdb

import java.io.IOException

import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.documentdb._
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import com.microsoft.azure.documentdb.PartitionKeyDefinition
import java.util



object TestWrite {

  // spark-submit --class org.jennings.estest.SendFileElasticsearch target/estest.jar planes00001 a1 9200 local[16] planes/planes

  // java -cp target/estest.jar org.jennings.estest.SendFileElasticsearchFile

  private val DEFAULT_FILENAME = "c:\\GitHub\\test-cosmos-db-spark\\data\\planes-small.json"
  private val DEFAULT_SPARK_MASTER = "local[8]"

  private val SERVICE_ENDPOINT = "https://a4iot-cosmos-db-sql.documents.azure.com:443/"
  private val MASTER_KEY = "21c1c35SmLuI80v8PfhLKW1rfAxqDW7wwWsTTdgvyXKn6KAILSpm6vQSUjVI14oJlGoYnMKN26FUgekehx15tw=="
  private val DATABASE_NAME = "PlanesDB2"
  private val COLLECTION_NAME = "PlanesCollection"
  private val PREFERRED_REGIONS = "West US;West US2;East US"
  private val WRITING_BATCH_SIZE = "100"

  //private val READ_QUERY = "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'"
  private val READ_QUERY = "SELECT * FROM c"

  private val FIRST_DOC_ID_TO_WRITE = 1000001
  private val NUM_OF_DOCS_TO_WRITE = 1000
  private val RUs = 15000


  def main(args: Array[String]): Unit = {

    val client = new DocumentClient(SERVICE_ENDPOINT, MASTER_KEY, new ConnectionPolicy, ConsistencyLevel.Session)


    val appName = getClass.getName

    val numargs = args.length
    if (numargs > 2) {
      System.err.println("Usage: TestWrite Filename SparkMaster")
      System.err.println("        Filename: Json File to Process")
      System.err.println("        SparkMaster: Spark Master (e.g. local[8] or - to use default)")
      System.exit(1)
    }

    //val Array(filename, sparkMaster) = args
    val filename = if (numargs > 0) args(0) else DEFAULT_FILENAME
    val sparkMaster = if (numargs > 1) args(1) else DEFAULT_SPARK_MASTER


    // deleting the DB
    deleteDatabase(client, DATABASE_NAME)
    //Thread.sleep(1000)


    // recreate the DB and collection
    createDatabase(client, DATABASE_NAME)
    createDocumentCollection(client, DATABASE_NAME, COLLECTION_NAME)
    //Thread.sleep(1000)


    // count
    queryCount(client, DATABASE_NAME, COLLECTION_NAME, "before")


    log("Sending " + filename + " to " + SERVICE_ENDPOINT + " using " + sparkMaster)

    val sparkConf = new SparkConf().setAppName(appName)
    sparkConf.set("spark.port.maxRetries", "50")
    if (sparkMaster.equalsIgnoreCase("-"))
      sparkConf.setMaster(DEFAULT_SPARK_MASTER)
    else
      sparkConf.setMaster(sparkMaster)

    val context = new SparkContext(sparkConf)
    val session: SparkSession = createOrGetDefaultSparkSession(context)
    val sqlContext = session.sqlContext


    // read from a JSON text file into a dataframe dataset
    val dataset = sqlContext.read.json(filename)
    dataset.show(5)

    // read from a JSON text file into a text datasets
    //val dataset = context.textFile(filename)
    //val count = dataset.count()
    //dataset.collect().foreach(println)


    // read from cosmosdb
    //readFromCosmosDB(session)


    // Write to cosmosdb
    //writeRDDToCosmosDB(session, dataset, SaveMode.Ignore)
    writeDatasetToCosmosDB(session, dataset, SaveMode.Ignore)


    // count
    queryCount(client, DATABASE_NAME, COLLECTION_NAME, "after")


    System.exit(0)
    //client.close()
  }

  def deleteDatabase(client: DocumentClient, databaseName: String): Unit = {
    client.deleteDatabase("/dbs/" + databaseName, null);
    log(String.format("Deleted database %s", databaseName))
  }

  @throws[DocumentClientException]
  @throws[IOException]
  private def createDatabase(client: DocumentClient, databaseName: String): Unit = {
    val databaseLink = String.format("/dbs/%s", databaseName)
    // check to verify a database with the id does not exist
    try {
      client.readDatabase(databaseLink, null)
      log(String.format("Found %s", databaseName))
    } catch {
      case de: DocumentClientException =>
        // if the database does not exist, create a new database
        if (de.getStatusCode == 404) {
          val database = new Database
          database.setId(databaseName)
          client.createDatabase(database, null)
          log(String.format("Created database %s", databaseName))
        }
        else throw de
    }
  }

  @throws[IOException]
  @throws[DocumentClientException]
  private def createDocumentCollection(client: DocumentClient, databaseName: String, collectionName: String): Unit = {
    val databaseLink = String.format("/dbs/%s", databaseName)
    val collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName)
    try {
      client.readCollection(collectionLink, null)
      log(String.format("Found %s", collectionName))
    } catch {
      case de: DocumentClientException =>
        // if the document collection does not exist, create a new collection
        if (de.getStatusCode == 404) {
          val collectionInfo = new DocumentCollection
          collectionInfo.setId(collectionName)

          // optionally, you can configure the indexing policy of a collection.
          // Here we configure collections for maximum query flexibility including string range queries.
          val index = new RangeIndex(DataType.String)
          index.setPrecision(-1)
          collectionInfo.setIndexingPolicy(new IndexingPolicy(Array[Index](index)))
          // DocumentDB collections can be reserved with throughput specified in request units/second.
          // 1 RU is a normalized request equivalent to the read of a 1KB document.
          // Here we create a collection with RUs (default is 400) RU/s.

          val partitionKeyDefinition = new PartitionKeyDefinition
          val paths = new util.ArrayList[String]
          paths.add("/id")
          partitionKeyDefinition.setPaths(paths)
          collectionInfo.setPartitionKey(partitionKeyDefinition)


          val requestOptions = new RequestOptions
          // Added this to try to get past RUs limit of 10,000; did not help
          //requestOptions.setPartitionKey(new PartitionKey("/id"))

          requestOptions.setOfferThroughput(RUs)
          client.createCollection(databaseLink, collectionInfo, requestOptions)
          log(String.format("Created %s", collectionName))
        } else {
          throw de
        }
    }
  }

  def queryCount(client: DocumentClient, databaseName: String, collectionName: String, caption: String): Unit = {
    // set some common query options
    val queryOptions = new FeedOptions
    queryOptions.setPageSize(-1)
    queryOptions.setEnableCrossPartitionQuery(true)
    val collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName)
    log("Running SQL query...")
    val queryResults = client.queryDocuments(collectionLink, "SELECT value COUNT(1) FROM PlanesCollection", queryOptions)
    import scala.collection.JavaConversions._
    for (result <- queryResults.getQueryIterable) {
      log(String.format("(count %s): %s", caption, result))
    }
  }

  def writeDatasetToCosmosDB(session: SparkSession, df: Dataset[_], saveMode: SaveMode): Unit = {
    // Configure connection to the sink collection
    val writeConfig = Config(Map(
      "Endpoint" -> SERVICE_ENDPOINT,
      "Masterkey" -> MASTER_KEY,
      "Database" -> DATABASE_NAME,
      "PreferredRegions" -> PREFERRED_REGIONS,
      "Collection" -> COLLECTION_NAME,
      "WritingBatchSize" -> WRITING_BATCH_SIZE))

    val stopper = Stopper()

    // Write the dataframe
    //df.write.cosmosDB(writeConfig)

    // Write the dataframe with save mode (e.g. Upsert)
    df.write.mode(saveMode).cosmosDB(writeConfig)

    // Alternatively, write from an RDD
    //df.rdd.saveToCosmosDB(writeConfig)

    stopper.logTime("writeDatasetToCosmosDB - ")
  }

  def writeRDDToCosmosDB(session: SparkSession, rdd: RDD[_], saveMode: SaveMode): Unit = {
    // Configure connection to the sink collection
    val writeConfig = Config(Map(
      "Endpoint" -> SERVICE_ENDPOINT,
      "Masterkey" -> MASTER_KEY,
      "Database" -> DATABASE_NAME,
      "PreferredRegions" -> PREFERRED_REGIONS,
      "Collection" -> COLLECTION_NAME,
      "WritingBatchSize" -> WRITING_BATCH_SIZE))

    val stopper: Stopper = Stopper()

    rdd.saveToCosmosDB(writeConfig)

    stopper.logTime("writeRDDToCosmosDB - ")

  }

  def readFromCosmosDB(session: SparkSession): Unit = {
    // Configure connection to your collection
    val readConfig = Config(Map(
      "Endpoint" -> SERVICE_ENDPOINT,
      "Masterkey" -> MASTER_KEY,
      "Database" -> DATABASE_NAME,
      "PreferredRegions" -> PREFERRED_REGIONS,
      "Collection" -> COLLECTION_NAME,
      "SamplingRatio" -> "1.0",
      "query_custom" -> READ_QUERY))


    val stopper: Stopper = Stopper()

    val sqlContext = session.sqlContext
    val coll = sqlContext.read.cosmosDB(readConfig)
    coll.createOrReplaceTempView("c")

    // Queries
    var query = "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.destination = 'SFO'"
    val df = session.sql(query)

    // Run DF query (count)
    val count = df.count()

    stopper.logTime("readFromCosmosDB - count: " + count + " - ")
  }

  def createOrGetDefaultSparkSession(sc: SparkContext): SparkSession = {
    val builder = SparkSession.builder().config(sc.getConf)
    val osName = System.getProperty("os.name")
    if (!StringUtils.isEmpty(osName) && osName.toLowerCase().contains("win")) {
      // The spark.sql.warehouse.dir parameter is to workaround an path issue with Spark on Windows
      builder.config("spark.sql.warehouse.dir", s"file:///${System.getProperty("user.dir")}")
    }
    builder.getOrCreate()
  }

  @throws[DocumentClientException]
  @throws[IOException]
  private def writeNoSpark(client: DocumentClient, firstId: Long = FIRST_DOC_ID_TO_WRITE, numOfDocs: Long = NUM_OF_DOCS_TO_WRITE): Unit = {
    val collectionLink = String.format("/dbs/%s/colls/%s", DATABASE_NAME, COLLECTION_NAME)
    val lastId = firstId + numOfDocs
    var id = firstId
    log(String.format("writing %s documents from %s to %s ...", numOfDocs.toString, firstId.toString, lastId.toString))
    val initialPlane = Plane.createPlane
    val stopper = new Stopper
    while (id < firstId + NUM_OF_DOCS_TO_WRITE) {
      val plane = initialPlane.withId(String.format("%s", id.toString))
      val response = writePlane(client, collectionLink, plane)
      id += 1
    }
    stopper.logTime(String.format("writing %s documents done - ", numOfDocs.toString))
  }


  private def writePlane(client: DocumentClient, collectionLink: String, plane: Plane): ResourceResponse[Document] = {
    try {
      client.createDocument(collectionLink, plane, new RequestOptions, true)
    } catch {
      case ex: Throwable =>
        null
    }
  }

  @throws[IOException]
  private def log(text: String): Unit = {
    System.out.println(text)
    //System.out.println("Press any key to continue ...");
    //System.in.read();
  }

}
