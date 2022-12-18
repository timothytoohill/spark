/*
This is the second stage of a batch-processing pipeline. It performs many functions, including:
1. Building both tabular and graph schemas in Cassandra (graph supported by DSE's Janus variant). 
2. Schemas are derived from DataFrames that have been loaded from pre-processed raw data files. 
3. Creates indexes in ElasticSearch using the same schemas and loads the indexes
4. Tracks ETL batch progress and supports stopping/resuming batch processing
5. Creates secondary and solr indexes in Cassandra where necessary 
*/
import spark.implicits._;
import scala.collection.JavaConverters._;
import scala.collection.mutable.{ListBuffer, ListMap, HashMap };
import scala.collection.immutable.{ Map };
import scala.Enumeration;
import scala.io.{Codec, Source};
import org.apache.spark.sql.{DataFrame, Row};
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction};
import org.apache.http.{HttpEntity, HttpResponse};
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods._;
import org.apache.http.entity._;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.spark.sql._;
import java.time.{Duration, LocalDateTime, LocalDate, LocalTime};
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder};
import java.time.temporal.ChronoField;
import java.io.{InputStream, ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream, BufferedReader, InputStreamReader};
import java.nio.charset._;
import com.datastax.driver.dse.{ DseCluster };
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder, AmazonS3URI, AmazonS3Client};
import com.amazonaws.services.s3.model._;
import com.amazonaws.auth.{EnvironmentVariableCredentialsProvider, BasicAWSCredentials, AWSStaticCredentialsProvider};
import com.amazonaws.{ClientConfiguration, Protocol};
import com.amazonaws.regions.{Region, Regions};

class S3DirIterator(bucket: String, prefix: String = "", s3Client: AmazonS3 = getS3Client, endsWithFilters: Array[String] = Array(".xml", ".csv")) extends Serializable {
    private var listing: ObjectListing = null;

    def getNextListing(): List[String] = {
        if (listing == null) {
            listing = s3Client.listObjects(bucket, prefix);
        } else {
            if (listing.isTruncated) {
                listing = s3Client.listNextBatchOfObjects(listing);
            } else {
                return Nil;
            }
        }
        var keys = listing.getObjectSummaries.asScala.map(_.getKey).toList;
        var results = ListBuffer[String]();
        if (endsWithFilters.length > 0) {
            endsWithFilters.foreach((filter) => {
                keys.foreach((key) => {
                    if (key.toUpperCase().endsWith(filter.toUpperCase())) {
                        if (results.contains(key)) {
                            
                        } else {
                            results += key;
                        }
                    }
                })
            })
        } else {
            return keys;
        }
        return results.toList;
    }
}

class ConcatNarravtiveStringsUDAF(stringColumnName: String, stringIndexColumnName: String, sep:String = " ") extends UserDefinedAggregateFunction {
    def inputSchema:StructType = StructType(StructField(stringColumnName, StringType) :: StructField(stringIndexColumnName, IntegerType) :: Nil);
    def bufferSchema:StructType = StructType(StructField("map", MapType(IntegerType, StringType)) :: Nil);
    def dataType:DataType = StringType;
    def deterministic:Boolean = true;
    def initialize(buffer:MutableAggregationBuffer): Unit = buffer(0) = Map[Integer, String]()

    private def concatStrings(map: Map[Integer, String]): String = {
        val sortedMap = Map(map.toSeq.sortBy(_._1):_*)
        var resultString = ""
        for ((k, v) <- sortedMap) {
            resultString += v.replaceAll("\n", " ") + sep
        }
        return resultString
    }

    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        var map = buffer.getAs[Map[Integer, String]](0)
        val strVal = input.getAs[String](0)
        val stringIndex = input.getAs[Integer](1)
        map += (stringIndex -> strVal)
        buffer(0) = map
    }
    
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        var map = buffer1.getAs[Map[Integer, String]](0)
        val toMerge = buffer2.getAs[Map[Integer, String]](0)
        
        for ((k, v) <- toMerge) {
            map += (k -> v)
        }
        buffer1(0) = map
    }

    def evaluate(buffer: Row): String = concatStrings(buffer.getAs[Map[Integer, String]](0)).trim()
}
val narrativeConcatenator = new ConcatNarravtiveStringsUDAF("NarrativeText", "NarrativeSequenceNumber", " ");

case class RelDef(name:String, sourceEntity:String, destEntity:String);
case class IndexDef(name:String, statement:String);

val cqlWait = 10000;

def waitMilli(milliseconds:Int) = {
    Thread.sleep(milliseconds);
}

val uuidUDF = udf((s:String) => java.util.UUID.nameUUIDFromBytes(s.getBytes).toString);
val modulusGrouping = udf((numGroups:Int, colKey:String) => ((colKey.hashCode() % numGroups).abs));

var lastPLN = LocalDateTime.now();
def pln(line: Any) = {
    val dt = LocalDateTime.now();
    val durationSinceLast = Duration.between(lastPLN, dt);
    lastPLN = LocalDateTime.now();
    val durStr = formatDuration(durationSinceLast);
    println("(" + dt.toString() + " (" + durStr + ")): " + line.toString());
}

def formatDuration(diff: Duration): String = {
    val s = diff.getSeconds();
    val n = diff.getNano();
    return "%02d:%02d:%02d:%03d".format(s / 3600, (s % 3600) / 60, (s % 60), (n / 1000000));    
}

def ensureEOL(str: String): String = {
    if (str.endsWith("\n")) {
        
    } else {
        return str + "\n";
    }
    return str;
}

def getS3Client: AmazonS3 = {
    var creds = new BasicAWSCredentials("", "");
    var clientConfig = new ClientConfiguration();
    clientConfig.setProtocol(Protocol.valueOf("HTTPS"));
    var client = new AmazonS3Client(creds, clientConfig);
    val endpoint = "s3-website.us-west-1.amazonaws.com";
    client.setEndpoint(endpoint);
    client.setRegion(Region.getRegion(Regions.Cloud));
    return client;
}

def getS3KeyFileName(key: String): String = {
    if (key.contains("/")) {
        return key.substring(key.lastIndexOf("/") + 1)
    }
    return ""
}

def getStrFromInputStream(is: InputStream):String = {
    val codec = Codec("UTF-8");
    codec.onMalformedInput(CodingErrorAction.IGNORE);
    codec.onUnmappableCharacter(CodingErrorAction.IGNORE);
    val pulledString = scala.io.Source.fromInputStream(is)(codec).mkString;
    return pulledString;
}

def writeStreamToS3(s3Client: AmazonS3, bucket: String, key: String, inputStream: InputStream, streamLength: Long, contentType: String = "plain/text") = {
    val metadata = new ObjectMetadata();
    metadata.setContentType(contentType);
    metadata.setContentLength(streamLength);
    val request = new PutObjectRequest(bucket, key, inputStream, metadata)
    s3Client.putObject(request)
}

def writeByteArrayToS3(s3Client: AmazonS3, bucket: String, key: String, bytes: Array[Byte], contentType: String = "plain/text") = {
    val inputStream = new ByteArrayInputStream(bytes);
    writeStreamToS3(s3Client, bucket, key, inputStream, bytes.length, contentType);
}

def writeStringToS3(s3Client: AmazonS3, bucket: String, key: String, content: String, contentType: String = "plain/text") = {
    val bytes = content.getBytes("UTF-8");
    writeByteArrayToS3(s3Client, bucket, key, bytes, contentType);
}

def deleteRDDFiles(bucket: String, locationKey: String, s3Client: AmazonS3): Unit = {
    val dirIterator = new S3DirIterator(bucket, locationKey, s3Client, Array());
    var listing = dirIterator.getNextListing();
    var msg = false;
    while (listing != Nil && listing.size > 0) {
        if (msg) { } else { pln("Deleting old " + bucket + "/" + locationKey + "..."); msg = true; }
        val paraDel = sc.parallelize(listing);
        paraDel.foreach((key) => {
            getS3Client.deleteObject(new DeleteObjectRequest(bucket, key));
        })
        listing = dirIterator.getNextListing(); 
    }
    if (msg) { pln("Done deleting old " + bucket + "/" + locationKey + "."); }
}

def getSortedRDDPartKeys(sourceBucket: String, sourceLocationKey: String, s3Client: AmazonS3): ListBuffer[String] = {
    val dirIterator = new S3DirIterator(sourceBucket, sourceLocationKey, s3Client, Array());
    var listing = dirIterator.getNextListing();
    var allKeys = ListBuffer[String]();
    
    while (listing != Nil && listing.size > 0) {
        listing.foreach((key) => {
            if (key.endsWith("_SUCCESS")) {
                
            } else {
                allKeys += key;
            }
        })
        listing = dirIterator.getNextListing(); 
    }
    
    allKeys = allKeys.sortWith(_.compareTo(_) < 0);
    
    return allKeys;
}

def addHeaderToFirstPartFile(sourceBucket: String, sourceLocationKey: String, s3Client: AmazonS3, header: String): Unit = {
    var allKeys = getSortedRDDPartKeys(sourceBucket, sourceLocationKey, s3Client);
    if (allKeys.size > 0) {
        val firstPartKey = allKeys(0);
        val headerBytes = ensureEOL(header).getBytes("UTF-8");
        val partInputStream = s3Client.getObject(new GetObjectRequest(sourceBucket, firstPartKey)).getObjectContent();
        val partBytes = IOUtils.toByteArray(partInputStream);
        var combinedArray = new Array[Byte](headerBytes.size + partBytes.size);
        
        Array.copy(headerBytes, 0, combinedArray, 0, headerBytes.size);
        Array.copy(partBytes, 0, combinedArray, headerBytes.size, partBytes.size);
        
        writeByteArrayToS3(s3Client, sourceBucket, firstPartKey, combinedArray);
    }
}

def concatenateS3RDDFiles(sourceBucket: String, sourceLocationKey: String, destBucket: String, destLocationKey: String, s3Client: AmazonS3): Unit = {
    pln("Concatenating RDD files from '" + sourceBucket + "/" + sourceLocationKey + "' to '" + destBucket + "/" + destLocationKey + "'...");

    var allKeys = getSortedRDDPartKeys(sourceBucket, sourceLocationKey, s3Client);

    if (allKeys.size > 0) {
        pln("There are " + allKeys.size.toString + " files to concatenate...");

        //beacuse s3 multipart uploads require a minimum file size of 5MB, we have to iterate over all partition files and ensure none are less than that.
        var index = 1;
        var mergeObjectIndex = 0;
        val s3MultipartMinSize = 6000000;
        while (index < allKeys.size) {
            var objSize = s3Client.getObjectMetadata(sourceBucket, allKeys(index)).getContentLength;
            if (objSize < s3MultipartMinSize) {
                objSize = s3Client.getObjectMetadata(sourceBucket, allKeys(mergeObjectIndex)).getContentLength;
                if (objSize > s3MultipartMinSize) {
                    mergeObjectIndex = index;
                } else {
                    pln("Merging '" + getS3KeyFileName(allKeys(index)) + "' to '" + getS3KeyFileName(allKeys(mergeObjectIndex)) + "' because it was too small for multipart uploads...");
                    val partInputStream1 = s3Client.getObject(new GetObjectRequest(sourceBucket, allKeys(mergeObjectIndex))).getObjectContent();
                    val partInputStream2 = s3Client.getObject(new GetObjectRequest(sourceBucket, allKeys(index))).getObjectContent();
                    val partBytes1 = IOUtils.toByteArray(partInputStream1);
                    val partBytes2 = IOUtils.toByteArray(partInputStream2);
                    var combinedArray = new Array[Byte](partBytes1.size + partBytes2.size);
                    
                    Array.copy(partBytes1, 0, combinedArray, 0, partBytes1.size);
                    Array.copy(partBytes2, 0, combinedArray, partBytes1.size, partBytes2.size);
                    
                    writeByteArrayToS3(s3Client, sourceBucket, allKeys(mergeObjectIndex), combinedArray);
                    
                    s3Client.deleteObject(sourceBucket, allKeys(index));
                    pln("Done merging '" + getS3KeyFileName(allKeys(index)) + "' to '" + getS3KeyFileName(allKeys(mergeObjectIndex)) + "'.");
                }
            } else {
                mergeObjectIndex = index;
            }
            index += 1;    
        }
        
        allKeys = getSortedRDDPartKeys(sourceBucket, sourceLocationKey, s3Client);

        pln("Performing concurrent multipart upload of " + allKeys.size.toString + " RDD partition files...");

        var keyMap = ListMap[Int, String]();
        index = 0;
        while (index < allKeys.size) {
            keyMap += (index -> allKeys(index))
            index += 1
        }

        val paraMap = sc.parallelize(keyMap.toSeq);

        val initMPRequest = new InitiateMultipartUploadRequest(destBucket, destLocationKey);
        val initMPResponse = s3Client.initiateMultipartUpload(initMPRequest);
        val uploadID = initMPResponse.getUploadId();

        class SerializablePartETag(val partNumber: Int, val eTag: String) extends Serializable { }
        
        val partETagsRDD = paraMap.map((km) => {
            val index = km._1;
            val key = km._2;
            val s3Client = getS3Client;
            val partSize = s3Client.getObjectMetadata(sourceBucket, key).getContentLength
            val inputStream = s3Client.getObject(new GetObjectRequest(sourceBucket, key)).getObjectContent();
            val uploadRequest = new UploadPartRequest().withBucketName(destBucket).withKey(destLocationKey).withUploadId(uploadID).withPartNumber(index + 1).withInputStream(inputStream).withPartSize(partSize);
            val uploadResult = s3Client.uploadPart(uploadRequest);
            val partETag = uploadResult.getPartETag();
            inputStream.close();
            
            val serializablePartETag = new SerializablePartETag(partETag.getPartNumber(), partETag.getETag());
            
            serializablePartETag;
        });

        val serializablePartETags = partETagsRDD.collect();
        var partETags = new java.util.ArrayList[PartETag]();
        serializablePartETags.foreach((spet) => {
           val partETag = new PartETag(spet.partNumber, spet.eTag);
           partETags.add(partETag);
        });

        val compMPRequest = new CompleteMultipartUploadRequest(destBucket, destLocationKey, uploadID, partETags);
        
        s3Client.completeMultipartUpload(compMPRequest);

        pln("Done performing concurrent multipart upload of " + allKeys.size.toString + " RDD partition files.");
    } else {
        pln("No RDD files to concatenate.");
    }

    pln("Done concatenating RDD files from '" + sourceBucket + "/" + sourceLocationKey + "' to '" + destBucket + "/" + destLocationKey + "'.");
}

def concatenateS3RDDFilesWithHeader(sourceBucket: String, sourceLocationKey: String, destBucket: String, destLocationKey: String, s3Client: AmazonS3, header: String): Unit = {
    var allKeys = getSortedRDDPartKeys(sourceBucket, sourceLocationKey, s3Client);
    if (allKeys.size > 0) {
        addHeaderToFirstPartFile(sourceBucket, sourceLocationKey, s3Client, header);
        concatenateS3RDDFiles(sourceBucket, sourceLocationKey, destBucket, destLocationKey, s3Client);
    }
}

def saveDataFrameAsOneCSV(s3Client: AmazonS3, df: DataFrame, destBucket: String, destLocationKey: String) = {
    pln("Saving dataframe to '" + destLocationKey + "'...");
    val sep = ",";
    val destLocationKeyTemp = destLocationKey + ".temp";
    var destURITemp = s3URIPrefix + destBucket + "/" + destLocationKeyTemp;
    val header = df.columns.mkString(sep);
    
    deleteRDDFiles(destBucket, destLocationKeyTemp, s3Client);
    
    df.write.format("csv").mode("overwrite").option("header", "false").option("escape", "\"").option("quoteAll", "true").save(destURITemp);
    
    concatenateS3RDDFilesWithHeader(destBucket, destLocationKeyTemp, destBucket, destLocationKey, s3Client, header);
    
    deleteRDDFiles(destBucket, destLocationKeyTemp, s3Client);
    
    pln("Done saving dataframe to '" + destLocationKey + "'.");
}

def getDSESession() = {
    var builder = DseCluster.builder();
    builder = builder.addContactPoint("x.x.x.189");
    builder = builder.addContactPoint("x.x.x.165");
    var cluster = builder.build();
    cluster.getConfiguration().getGraphOptions().setGraphName(dataModelKeySpace);
    var session = cluster.connect();
    (cluster, session);
}

def executeCQL(cql:String) = {
    val (cluster, session) = getDSESession();
    pln(s"----Running CQL query '$cql'...");
    val result = session.execute(cql);
    session.close();
    cluster.close();
    pln(s"----Query result: '$result'.");
}

def executeGraph(graphStatement:String) = {
    val (cluster, session) = getDSESession();
    pln(s"----Running graph query '$graphStatement'...");
    val result = session.executeGraph(graphStatement);
    session.close();
    cluster.close();
    pln(s"----Query result: '$result'.");
}

def getElasticIndexCount(indexName:String):Long = {
    val req = new HttpGet("http://" + elasticEndpoint + "/" + indexName + "/_count");
    req.addHeader("Accept","text/plain");
    req.addHeader("Connection","close");
    val clientBuilder = HttpClientBuilder.create();
    val client = clientBuilder.build();
    val response = client.execute(req);
    var responseStr = getStrFromInputStream(response.getEntity().getContent());
    var df = spark.read.json(Seq(responseStr).toDS);
    if (df.columns.contains("count")) {
        return df.select("count").first.getLong(0);    
    } 
    return 0;
}

def deleteElasticIndex(indexName:String) = {
    if (enableElastic && elasticDeleteIndexes) {
        pln(s"Deleting Elastic index $indexName...");
        val req = new HttpDelete("http://" + elasticEndpoint + "/" + indexName);
        req.addHeader("Accept","text/plain");
        req.addHeader("Connection","close");
        val clientBuilder = HttpClientBuilder.create();
        val client = clientBuilder.build();
        val response = client.execute(req);
        var responseStr = getStrFromInputStream(response.getEntity().getContent());
        waitMilli(60000);
        pln(s"Done deleting Elastic index $indexName.");
    }
}

def saveDFToElastic(dataFrame:DataFrame, indexName:String, count:Long = 0L) = {
    var df = dataFrame;
    if (enableElastic) {
        pln(s"Saving dataframe to elastic index '$indexName'...");
        var dfCount = count;
        if (dfCount == 0) { dfCount = df.count(); }
        var stringColumns = df.schema.toList.filter(x => x.dataType.isInstanceOf[StringType]).map(c => c.name);
        stringColumns.foreach((colName) => {
            df = df.withColumn(colName, when(col(colName) === lit(""), null).otherwise(col(colName)));
        });
        var timestampColumns = df.schema.toList.filter(x => x.dataType.isInstanceOf[TimestampType]).map(c => c.name);
        timestampColumns.foreach((colName) => {
            df = df.withColumn(colName, regexp_replace(col(colName).cast(StringType), lit(" "), lit("T")));
        });
        var dateColumns = df.schema.toList.filter(x => x.dataType.isInstanceOf[DateType]).map(c => c.name);
        dateColumns.foreach((colName) => {
            df = df.withColumn(colName, col(colName).cast(StringType));
        });
        val indexCount = getElasticIndexCount(indexName);
        pln(s"Data frame count: $dfCount, index count: $indexCount...");
        var esConf = Map("es.nodes" -> elasticEndpoint, "es.port" -> "80", "es.write.operation" -> "upsert", "es.mapping.id" -> "ID", "es.nodes.wan.only" -> "true", "es.field.read.empty.as.null" -> "true");
        df.saveToEs(indexName, esConf);
        pln(s"Done saving index $indexName.");
    }
}

def loadTracking(destKeySpace:String) = {
    if (enableTracking) {
        ensureKeySpace(trackingKeySpace);
        if (doesSparkTableExist(trackingKeySpace, destKeySpace)) {

        } else {
            executeCQL(s"""CREATE TABLE IF NOT EXISTS $trackingKeySpace.$destKeySpace (source_data_set varchar, dest_table varchar, type varchar, count bigint, reload boolean, PRIMARY KEY (source_data_set, dest_table)) WITH VERTEX LABEL $destKeySpace;""");
            refreshSparkTable(trackingKeySpace, destKeySpace);
        }
    }
}

def saveTrackingInfoToCSV(keySpace:String, saveFinal:Boolean = false) = {
    if (enableTracking) {
        val destBucket = "timt-dev";
        val prefix = "etl-tracking";
        val name1 = "counts_by_raw_source.csv";
        val name2 = "counts_by_model_rollup.csv";
        val name3 = "counts_rollup.csv";
        val s3URIPrefix2 = "s3a:";
        var df = getBaseTableDF(trackingKeySpace, keySpace);
        var dfTypeTotals = df.groupBy("dest_table", "type").agg(sum("count").as("total_count"));
        var dfTotals = df.groupBy("type").agg(sum("count").as("total_count"));
        df.orderBy("type").repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(s"$s3URIPrefix2//$destBucket/$prefix/$keySpace/$name1");
        dfTypeTotals.orderBy("type").repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(s"$s3URIPrefix2//$destBucket/$prefix/$keySpace/$name2");
        dfTotals.orderBy("type").repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(s"$s3URIPrefix2//$destBucket/$prefix/$keySpace/$name3");

        if (saveFinal) {
            saveDataFrameAsOneCSV(getS3Client, df, destBucket, s"$prefix/$keySpace/final_$name1");
            saveDataFrameAsOneCSV(getS3Client, dfTypeTotals, destBucket, s"$prefix/$keySpace/final_$name2");
            saveDataFrameAsOneCSV(getS3Client, dfTotals, destBucket, s"$prefix/$keySpace/final_$name3");
        }
    }
}

def trackingShouldLoad(keySpace:String, sourceDataSet:String, destTable:String) = {
    var shouldLoad = true;
    if (enableTracking && trackingSkipLoaded) {
        var df = getBaseTableDF(trackingKeySpace, keySpace);
        df = df.filter(col("source_data_set") === lit(sourceDataSet) && col("dest_table") === lit(destTable) && col("reload") === lit(false));
        val count = df.count();
        if (count > 0) {
            shouldLoad = false;
        }
    }
    shouldLoad;
}

def updateTracking(keySpace:String, sourceDataSet:String, destTable:String, thingType:String, count:Long) = {
    executeCQL(s"UPDATE $trackingKeySpace.$keySpace SET type = '$thingType', count = $count, reload = false WHERE source_data_set = '$sourceDataSet' AND dest_table = '$destTable'");
    saveTrackingInfoToCSV(keySpace);
}

def resetTracking(keySpace:String, sourceDataSet:String = "", destTable:String = "") = {
    var df = getBaseTableDF(trackingKeySpace, keySpace);
    if (sourceDataSet.length > 0) {
        df = df.filter(col("source_data_set") === lit(sourceDataSet));
    }
    if (destTable.length > 0) {
        df = df.filter(col("dest_table") === lit(destTable));
    }
    df = df.withColumn("reload", lit(true));
    saveDataFrame(df, trackingKeySpace, keySpace);
}

def getUniqueColNames(columnsUsed:Array[Array[String]]) = {
    var uniqueColsUsed = ListBuffer[String]();
    columnsUsed.foreach((arr) => {
        arr.foreach((colName) => {
            if (uniqueColsUsed.contains(colName)) { } else {
                uniqueColsUsed.append(colName);
            }
        })
    })
    var selectCols = Array(uniqueColsUsed:_*);
    selectCols;
}

def cacheDF(df:DataFrame) = {
    var cachedDF = df.persist();
    cachedDF;
}

def clearAllCached() = {
    pln("Cleaaring all cached/persisted RDDs/DataFrames...");;
    spark.catalog.clearCache();
    val rdds = sc.getPersistentRDDs;
    for ((id, rdd) <- rdds) {
        rdd.unpersist();
    }
    pln("Done cleaaring all cached/persisted RDDs/DataFrames.");;
}

def selectDistinctRows(df:DataFrame, colName:String) = {
    pln(s"""Selecting only distinct rows by $colName...""");
    var oldDF = cacheDF(df); 
    val preCount = oldDF.count();
    var newDF = oldDF.dropDuplicates(colName);
    newDF = cacheDF(newDF);
    oldDF = oldDF.unpersist();
    val postCount = newDF.count();
    pln(s"""Count before/after distinct select: $preCount/$postCount.""");
    pln(s"""Done selecting only distinct rows by $colName.""");
    newDF;
}

def applyDebugLimit(df:DataFrame) = {
    var limitedDF = df;
    if (isDebug && debugRecordsToLoad > 0) {
        pln(s"Applying debug limit on dataframe to $debugRecordsToLoad records.")
        limitedDF = limitedDF.limit(debugRecordsToLoad);
    }
    limitedDF;
}

def getBaseTableDF(keySpace:String, tableName:String) = {
    var ks = keySpace;
    if (ks == "") ks = dataModelKeySpace;
    var tableDF = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace", ks).option("table", tableName).load();
    if (tableDF.columns.contains("solr_query")) {
        tableDF = tableDF.drop("solr_query");
    }
    tableDF;
}

def getTableDF(keySpace:String, tableName:String, selectColumns:Array[String] = Array[String](), allowDebugLimit:Boolean = true, selectDistinct:Boolean = false, distinctColumn:String = "ID") = {
    var tableDF = getBaseTableDF(keySpace, tableName);
    if (selectColumns.length > 0) {
        tableDF = tableDF.select(selectColumns.head, selectColumns.tail:_*);
    }
    if (allowDebugLimit) {
        tableDF = applyDebugLimit(tableDF);
    }

    pln(s"""Loading '$keySpace.$tableName' table...""");
    tableDF = cacheDF(tableDF); 
    if (selectDistinct) {
        tableDF = selectDistinctRows(tableDF, distinctColumn);
    }
    pln(s"""Done loading '$keySpace.$tableName' table.""");

    tableDF;
}

def saveDataFrame(df:DataFrame, keySpace:String, tableName:String) = {
    df.write.format("org.apache.spark.sql.cassandra").option("keyspace", keySpace).option("table", tableName).mode("append").save();
}

def subsetDataFrame(df:DataFrame, colKey:String, numSubsets:Int = 10) = {
    pln(s"""Splitting dataframe to $numSubsets subsets...""");
    var newDFs = new ListBuffer[DataFrame]();
    var newDF = df.withColumn("groupingColumn", modulusGrouping(lit(numSubsets), col(colKey)));
    newDF = newDF.orderBy("groupingColumn");
    newDF = cacheDF(newDF); 
    df.unpersist();
    var counter = 0;
    var counts = ListBuffer[Long]();
    while (counter < numSubsets) {
        var subset = newDF.filter(col("groupingColumn") === counter);
        subset = subset.drop("groupingColumn");
        subset = cacheDF(subset);
        val c = subset.count();
        counts.append(c);
        if (c > 0) {
            newDFs.append(subset)
        } 
        counter += 1;
    }
    newDF.unpersist();
    pln(s"""Done splitting dataframe to $numSubsets subsets. Counts: """ + counts.mkString(",") + ".");
    newDFs;
}

def ensureKeySpace(keySpaceName:String = "") {
    var keySpace = dataModelKeySpace;
    if (keySpaceName.length > 0) keySpace = keySpaceName;
    var keySpaceCreationScript = s"CREATE KEYSPACE IF NOT EXISTS $keySpace WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'DSE_Prod' : 3 } AND graph_engine = 'Core';";

    refreshSparkDatabase(keySpace);

    pln(s"Checking for keyspace '$keySpace'...");
    
    var keySpaceColumnName = "databaseName";
    var keySpacesDF = spark.sql("SHOW SCHEMAS");
    keySpacesDF = keySpacesDF.select(keySpaceColumnName);
    keySpacesDF = keySpacesDF.filter(col(keySpaceColumnName) === keySpace);
    var keySpaces = keySpacesDF.collect().map(row => row.getString(0));
    
    if (keySpaces.contains(keySpace)) {
        pln("Keyspace found.");
    } else {
        pln("Keyspace not found. Creating...");
        executeCQL(keySpaceCreationScript);
        waitMilli(cqlWait);
        pln("Done creating keyspace.");
    }
}

def getCQLColumnTypeFromDataFrameColumn(df:DataFrame, colName:String) = {
    var cqlColType = "";
    if (df.columns.contains(colName)) {
        cqlColType = df.schema(colName).dataType match {
            case StringType => "varchar";
            case ByteType => "tinyint";
            case ShortType => "smallint";
            case IntegerType => "int";
            case LongType => "bigint";
            case FloatType => "float";
            case DoubleType => "double";
            case BinaryType => "blob";
            case BooleanType => "boolean";
            case TimestampType => "timestamp";
            case DateType => "date";
            case _ : NumericType => "decimal";
            case _ => "varchar";
        }
    }
    cqlColType;
}

def getEntityTableName(entityName:String) = {
    var tableName = entityName;
    tableName;
}

def getRelationshipTableName(relationship:RelDef, isOtherDirection:Boolean = false) = {
    var relationshipName = relationship.name; 
    var sourceEntity = relationship.sourceEntity;
    var destEntity = relationship.destEntity;
    var tableName = s"""$sourceEntity""" + s"""__$relationshipName""" + s"""__$destEntity""";
    if (isOtherDirection) {
        tableName = s"""$destEntity""" + s"""__$relationshipName""" + s"""__$sourceEntity""";
    }
    tableName;
}

def refreshSparkDatabase(keySpace:String) = {
    spark.sql(s"DROP DATABASE IF EXISTS $keySpace CASCADE");
    refreshSparkTable(keySpace, "doesntmatterxyz"); //force it to see if keyspace exists and update metastore
}

def refreshSparkTable(keySpace:String, tableName:String) = {
    try {
        val testDF = spark.sql(s"SELECT * FROM $keySpace.$tableName LIMIT 1");
        val count = testDF.count();        
        spark.catalog.refreshTable(tableName);
    } catch { case _ : Throwable => }
}

def doesSparkTableExist(keySpace:String, tableName:String) = {
    refreshSparkTable(keySpace, tableName);
    spark.sql(s"USE $keySpace");
    var tablesDF = spark.sql("SHOW TABLES");
    tablesDF = tablesDF.select("tableName");
    tablesDF = tablesDF.filter(col("isTemporary") === false && col("tableName") === tableName);
    val doesExist = (tablesDF.count() > 0);
    doesExist;
}

def ensureTableColumnsForDataFrame(df:DataFrame, existingTableDF:DataFrame, keySpace:String, tableName:String, ignoreColumns:Array[String]) = {
    var modified = false;
    var dfColumns = df.columns.clone();
    scala.util.Sorting.quickSort(dfColumns);
    dfColumns.foreach((colName) => {
        if (ignoreColumns.contains(colName)) {

        } else {
            val colType = getCQLColumnTypeFromDataFrameColumn(df, colName);
            val existingTableColType = getCQLColumnTypeFromDataFrameColumn(existingTableDF, colName);
            if (colType == existingTableColType) {

            } else {
                modified = true;
                if (existingTableDF.columns.contains(colName)) {
                    pln(s"Column $tableName.$colName exists but is the wrong type. Dropping...");
                    executeCQL(s"ALTER TABLE $keySpace.$tableName DROP $colName;");
                    waitMilli(cqlWait);
                }           
                pln(s"Adding column $colName to $tableName...");
                executeCQL(s"ALTER TABLE $keySpace.$tableName ADD $colName $colType;");
                if (isDebug) {} else {
                    waitMilli(cqlWait);
                }
            }
        }
    });
    
    if (modified) {
        if (isDebug) {
            waitMilli(cqlWait);
        } else {
            waitMilli(cqlWait * 3);
        }
        spark.catalog.refreshTable(tableName);
        pln(s"Refreshed spark metatdta for $keySpace.$tableName after changes.");
    }
}

def ensureEntityTableForDataFrame(df:DataFrame, entityName:String) = {
    var keySpace = dataModelKeySpace;
    val tableName = entityName;
    val labelName = entityName;

    pln(s"Ensuring table '$keySpace.$tableName' for '$entityName' entity data frame...");
    if (doesSparkTableExist(keySpace, tableName)) {
        pln(s"Table exists.");
    } else {
        pln(s"Table does not exist. Creating...");
        executeCQL(s"""CREATE TABLE IF NOT EXISTS $keySpace.$tableName ("ID" uuid, PRIMARY KEY ("ID")) WITH VERTEX LABEL $labelName;""");
        if (isDebug) {
            waitMilli(cqlWait);
        } else {
            waitMilli(cqlWait * 3);
        }
        pln(s"Done creating table.");
    }
    
    spark.catalog.refreshTable(tableName);
    pln(s"Refreshed spark metadata for $keySpace.$tableName.");

    var existingTableDF = getBaseTableDF(keySpace, tableName);
    ensureTableColumnsForDataFrame(df, existingTableDF, keySpace, tableName, Array("ID"));

    pln(s"Done ensuring table '$keySpace.$tableName' for '$entityName' entity data frame.");
}

def ensureRelationshipTableForDataFrame(df:DataFrame, relationship:RelDef) = {
    var keySpace = dataModelKeySpace;
    var relationshipName = relationship.name; 
    var sourceEntity = relationship.sourceEntity;
    var destEntity = relationship.destEntity;
    val tableName = getRelationshipTableName(relationship);
    val labelName = tableName;

    pln(s"Ensuring table '$keySpace.$tableName' for '$relationshipName' relationship data frame...");
    if (doesSparkTableExist(keySpace, tableName)) {
        pln(s"Table exists.");
    } else {
        pln(s"Table does not exist. Creating...");
        executeCQL(s"""CREATE TABLE $keySpace.$tableName ("out_ID" uuid, "in_ID" uuid, PRIMARY KEY ("out_ID", "in_ID")) WITH CLUSTERING ORDER BY ("in_ID" ASC) AND EDGE LABEL $labelName FROM $sourceEntity("out_ID") TO $destEntity("in_ID");""");
        if (isDebug) {
            waitMilli(cqlWait);
        } else {
            waitMilli(cqlWait * 3);
        }
        pln(s"Done creating table.");
    }
    
    spark.catalog.refreshTable(tableName);
    pln(s"Refreshed spark metadata for $keySpace.$tableName.");

    var existingTableDF = getBaseTableDF(keySpace, tableName);
    ensureTableColumnsForDataFrame(df, existingTableDF, keySpace, tableName, Array("inV", "outV"));

    pln(s"Done ensuring table '$keySpace.$tableName' for '$relationshipName' relationship data frame.");
}

def reverseEdgesDirection(edgesFunc:() => (String, ListBuffer[DataFrame])) = {
    val (sourceTable, dfs) = edgesFunc();
    var edgeDFs = ListBuffer[DataFrame]();

    dfs.foreach((df) => {
        var revDF = df;
        revDF = revDF.withColumnRenamed("out_ID", "temp_ID");
        revDF = revDF.withColumnRenamed("in_ID", "out_ID");
        revDF = revDF.withColumnRenamed("temp_ID", "in_ID");
        edgeDFs.append(revDF);
    });
    
    (sourceTable, edgeDFs);
}

def getModelEdgeDataFrames() = {
    var list = HashMap[RelDef, () => (String, ListBuffer[DataFrame])]();
    var keySpace = dataModelKeySpace;
    var selectCols = Array[String]();

    def getIIP() = {
        var edgeDFs = ListBuffer[DataFrame]();

        selectCols = Array("ID");
        var df = getTableDF(keySpace, "individual", selectCols, true); 
        df = df.withColumn("out_ID", col("ID"));   
        df = df.withColumnRenamed("ID", "in_ID");
        df = df.withColumn("when_associated", current_timestamp());
        edgeDFs.append(df);

        (s"$dataModelKeySpace.individual", edgeDFs);
    }

    def getOIP() = {
        var edgeDFs = ListBuffer[DataFrame]();

        selectCols = Array("ID");
        var df = getTableDF(keySpace, "organization", selectCols, true); 
        df = df.withColumn("out_ID", col("ID"));   
        df = df.withColumnRenamed("ID", "in_ID");
        df = df.withColumn("when_associated", current_timestamp());
        edgeDFs.append(df);

        (s"$dataModelKeySpace.organization", edgeDFs);
    }

    list += (RelDef("is", "individual", "entity") -> getIIP _);
    list += (RelDef("is", "organization", "entity") -> getOIP _);

    list;
}

def getZSAEdgeDataFrames() = {
    var list = HashMap[RelDef, () => (String, ListBuffer[DataFrame])]();
    var keySpace = "zsa_raw_data";
    var selectCols = Array[String]();

    def getPAWI() = {
        var edgeDFs = ListBuffer[DataFrame]();

        selectCols = Array("ID", "DocumentID", "SystemEntryDate");
        var entityDF = getTableDF(keySpace, "Entity", selectCols, true); 
        entityDF = entityDF.withColumnRenamed("ID", "out_ID");
        entityDF = entityDF.withColumnRenamed("SystemEntryDate", "when_associated");

        selectCols = Array("ID", "DocumentID");
        var ipDF = getTableDF(keySpace, "MainIPAddress", selectCols, false); 
        ipDF = ipDF.withColumnRenamed("ID", "in_ID");
        ipDF = ipDF.withColumnRenamed("DocumentID", "ChildDocumentID");

        var dfSubsets = subsetDataFrame(entityDF, "out_ID", largeTableSubsets);
        dfSubsets.foreach((subset) => {
            var joinedDF = subset.join(ipDF, col("DocumentID") === col("ChildDocumentID"), "left_outer");
            joinedDF = joinedDF.select("out_ID", "in_ID", "when_associated");
            joinedDF = joinedDF.na.drop(Seq("out_ID", "in_ID"));
            edgeDFs.append(joinedDF);
        });

        ("zsa_raw_data.entity", edgeDFs);
    }

    def getPHN() = {
        var edgeDFs = ListBuffer[DataFrame]();

        selectCols = Array("ID", "DocumentID", "SystemEntryDate");
        var entityDF = getTableDF(keySpace, "Entity", selectCols, true); 
        entityDF = entityDF.withColumnRenamed("ID", "out_ID");
        entityDF = entityDF.withColumnRenamed("SystemEntryDate", "when_associated");

        var (sourceParentTable, narrativeDFs) = getZSANarrativeDataFrames();
        var narrativeDF = narrativeDFs(0);
        narrativeDF = narrativeDF.withColumnRenamed("ID", "in_ID");

        var dfSubsets = subsetDataFrame(entityDF, "out_ID", largeTableSubsets);
        dfSubsets.foreach((subset) => {
            var joinedDF = subset.join(narrativeDF, col("DocumentID") === col("zsa_document_id"), "left_outer");
            joinedDF = joinedDF.select("out_ID", "in_ID", "zsa_document_id", "when_associated");
            joinedDF = joinedDF.na.drop(Seq("out_ID", "in_ID"));
            edgeDFs.append(joinedDF);
        });

        ("zsa_raw_data.mainnarrativeinformation", edgeDFs);
    }

    def getAHN() = {
        var edgeDFs = ListBuffer[DataFrame]();

        selectCols = Array("ID", "DocumentID", "SystemEntryDate");
        var mainDF = getTableDF(keySpace, "Main", selectCols, true); 
        mainDF = mainDF.withColumnRenamed("ID", "out_ID");
        mainDF = mainDF.withColumnRenamed("SystemEntryDate", "when_associated");

        var (sourceParentTable, narrativeDFs) = getZSANarrativeDataFrames();
        var narrativeDF = narrativeDFs(0);
        narrativeDF = narrativeDF.withColumnRenamed("ID", "in_ID");

        var dfSubsets = subsetDataFrame(mainDF, "out_ID", largeTableSubsets);
        dfSubsets.foreach((subset) => {
            var joinedDF = subset.join(narrativeDF, col("DocumentID") === col("zsa_document_id"), "left_outer");
            joinedDF = joinedDF.select("out_ID", "in_ID", "zsa_document_id", "when_associated");
            joinedDF = joinedDF.na.drop(Seq("out_ID", "in_ID"));
            edgeDFs.append(joinedDF);
        });

        ("zsa_raw_data.mainnarrativeinformation", edgeDFs);
    }

    def getPAWA() = {
        var edgeDFs = ListBuffer[DataFrame]();

        //entity associated_with account
        selectCols = Array("ID", "EntityID", "AccountID", "EntityAccountAssociationTypeCodeDescription", "SystemEntryDate");
        var accountAssociationDF = getTableDF(keySpace, "EntityAccountAssociation", selectCols, true); 
        accountAssociationDF = accountAssociationDF.withColumnRenamed("SystemEntryDate", "when_associated");
        accountAssociationDF = accountAssociationDF.withColumnRenamed("EntityAccountAssociationTypeCodeDescription", "description");
        
        selectCols = Array("ID", "EntityID");
        var entityDF = getTableDF(keySpace, "Entity", selectCols, false); 
        entityDF = entityDF.withColumnRenamed("ID", "out_ID");
        entityDF = entityDF.withColumnRenamed("EntityID", "ChildEntityID");
        
        selectCols = Array("ID", "AccountID");
        var accountDF = getTableDF(keySpace, "Account", selectCols, false); 
        accountDF = accountDF.withColumnRenamed("ID", "in_ID");
        accountDF = accountDF.withColumnRenamed("AccountID", "ChildAccountID");
        
        var dfSubsets = subsetDataFrame(accountAssociationDF, "ID", largeTableSubsets);
        dfSubsets.foreach((subset) => {
            var joinedDF = subset.join(entityDF, col("EntityID") === col("ChildEntityID"), "left_outer");
            joinedDF = joinedDF.join(accountDF, col("AccountID") === col("ChildAccountID"), "left_outer");
            joinedDF = joinedDF.select("out_ID", "in_ID", "description", "when_associated");
            joinedDF = joinedDF.na.drop(Seq("out_ID", "in_ID"));
            edgeDFs.append(joinedDF);
        });

        ("zsa_raw_data.entityaccountassociation", edgeDFs);
    }
    
    def getAAWA() = {
        var edgeDFs = ListBuffer[DataFrame]();

        selectCols = Array("ID", "DocumentID", "SavedDate");
        var zsaDF1 = getTableDF(keySpace, "Main", selectCols, true);

        selectCols = Array("ID", "DocumentID");
        var zsaDF2 = getTableDF(keySpace, "Account", selectCols, false); 
        
        zsaDF1 = zsaDF1.withColumnRenamed("ID", "out_ID");
        zsaDF1 = zsaDF1.withColumnRenamed("SavedDate", "when_associated");
        
        zsaDF2 = zsaDF2.withColumnRenamed("ID", "in_ID");
        zsaDF2 = zsaDF2.withColumnRenamed("DocumentID", "AccountDocumentID");
        
        var dfSubsets = subsetDataFrame(zsaDF1, "out_ID", largeTableSubsets);
        dfSubsets.foreach((subset) => {
            var joinedDF = subset.join(zsaDF2, col("DocumentID") === col("AccountDocumentID"))
            joinedDF = joinedDF.select("in_ID", "out_ID", "when_associated");
            joinedDF = joinedDF.na.drop(Seq("out_ID", "in_ID"));
            edgeDFs.append(joinedDF);
        });

        ("zsa_raw_data.account", edgeDFs);
    }
    
    def getAAWP() = {
        var edgeDFs = ListBuffer[DataFrame]();

        selectCols = Array("ID", "MainID", "SavedDate");
        var zsaDF1 = getTableDF(keySpace, "Main", selectCols, true);

        selectCols = Array("ID", "MainID");
        var zsaDF2 = getTableDF(keySpace, "Entity", selectCols, false); 

        zsaDF1 = zsaDF1.withColumnRenamed("ID", "out_ID");
        zsaDF1 = zsaDF1.withColumnRenamed("SavedDate", "when_associated");
        
        zsaDF2 = zsaDF2.withColumnRenamed("ID", "in_ID");
        zsaDF2 = zsaDF2.withColumnRenamed("MainID", "EntityMainID");
        
        var dfSubsets = subsetDataFrame(zsaDF1, "out_ID", largeTableSubsets);
        dfSubsets.foreach((subset) => {
            var joinedDF = subset.join(zsaDF2, col("MainID") === col("EntityMainID"));
            joinedDF = joinedDF.select("out_ID", "in_ID", "when_associated");
            joinedDF = joinedDF.na.drop(Seq("out_ID", "in_ID"));
            edgeDFs.append(joinedDF);
        });

        ("zsa_raw_data.entity", edgeDFs);
    }

    def getACWA() = {
        var edgeDFs = ListBuffer[DataFrame]();
        
        selectCols = Array("ID", "MainID", "SavedDate");
        var zsaDF1 = getTableDF(keySpace, "Main", selectCols, true); 

        selectCols = Array("ID", "MainID");
        var zsaDF2 = getTableDF(keySpace, "Objects", selectCols, false); 
        
        zsaDF1 = zsaDF1.withColumnRenamed("ID", "out_ID");
        zsaDF1 = zsaDF1.withColumnRenamed("SavedDate", "when_associated");

        zsaDF2 = zsaDF2.withColumnRenamed("ID", "in_ID");
        zsaDF2 = zsaDF2.withColumnRenamed("MainID", "ObjectAttributeMainID");

        var dfSubsets = subsetDataFrame(zsaDF1, "out_ID", largeTableSubsets);
        dfSubsets.foreach((subset) => {
            var joinedDF = subset.join(zsaDF2, col("MainID") === col("ObjectAttributeMainID"));
            joinedDF = joinedDF.select("out_ID", "in_ID", "when_associated");
            edgeDFs.append(joinedDF);
        });

        ("zsa_raw_data.objects", edgeDFs);
    }    

    list += (RelDef("associated_with", "entity", "ip_address") -> getPAWI _);
    list += (RelDef("has", "entity", "narrative") -> getPHN _);
    list += (RelDef("for", "narrative", "entity") -> (() => reverseEdgesDirection(getPHN _)));
    list += (RelDef("has", "main", "narrative") -> getAHN _);
    list += (RelDef("for", "narrative", "main") -> (() => reverseEdgesDirection(getAHN _)));
    list += (RelDef("associated_with", "entity", "account") -> getPAWA _);
    list += (RelDef("associated_with", "account", "entity") -> (() => reverseEdgesDirection(getPAWA _)));
    list += (RelDef("associated_with", "main", "account") -> getAAWA _);
    list += (RelDef("associated_with", "account", "main") -> (() => reverseEdgesDirection(getAAWA _)));
    list += (RelDef("associated_with", "main", "entity") -> getAAWP _);
    list += (RelDef("associated_with", "entity", "main") -> (() => reverseEdgesDirection(getAAWP _)));
    list += (RelDef("conducted_with", "main", "object") -> getACWA _);

    list;
}

def getGTEdgeDataFrames() = {
    var list = HashMap[RelDef, () => (String, ListBuffer[DataFrame])]();
    var keySpace = "gt_raw_data";
    var selectCols = Array[String]();

    def getIQC() = {
        var edgeDFs = ListBuffer[DataFrame]();
        
        // ips
        selectCols = Array("ID", "IP_ADDRESS", "INQUIRY_DATETIME");
        var ipDF = getTableDF(keySpace, "gt_data", selectCols, true); 
        ipDF = ipDF.filter(col("IP_ADDRESS") !== "")
        ipDF = ipDF.withColumn("out_ID", col("ID"));
        ipDF = ipDF.withColumnRenamed("ID", "in_ID");
        ipDF = ipDF.withColumn("when_inquired", to_timestamp(col("INQUIRY_DATETIME"), "dd/MM/yyyy HH:mm:ss"));
        ipDF = ipDF.withColumn("when_associated", col("when_inquired")); 
        ipDF = ipDF.drop("IP_ADDRESS", "INQUIRY_DATETIME");       
        edgeDFs.append(ipDF);

        ("gt_raw_data.gt_data", edgeDFs);
    }   

    def getVSC() = {
        var edgeDFs = ListBuffer[DataFrame]();
        
        // ips
        selectCols = Array("ID", "VESSEL_NAME", "LOADTIMESTAMP");
        var df = getTableDF(keySpace, "gt_data", selectCols, true); 
        df = df.filter(col("VESSEL_NAME") !== "")
        df = df.withColumn("out_ID", col("ID"));
        df = df.withColumnRenamed("ID", "in_ID");
        df = df.withColumn("when_loaded", to_timestamp(col("LOADTIMESTAMP"), "dd/MM/yyyy HH:mm:ss"));
        df = df.withColumn("when_associated", col("when_loaded"));        
        edgeDFs.append(df);

        ("gt_raw_data.gt_data", edgeDFs);
    }   
    
    def getPAWC() = {
        var edgeDFs = ListBuffer[DataFrame]();

        selectCols = Array("original_ID");
        var entityDF = getTableDF(dataModelKeySpace, "entity", selectCols, true); 
        entityDF = entityDF.na.drop(Array("original_ID"));

        selectCols = Array("ID");
        var containerDF = getTableDF(dataModelKeySpace, "container", selectCols, false); 

        // Join the tables based on ID
        var dfSubsets = subsetDataFrame(entityDF, "original_ID", largeTableSubsets);
        dfSubsets.foreach((subset) => {
            var joinedDF = subset.join(containerDF, col("original_ID") === col("ID"));
            joinedDF = joinedDF.withColumnRenamed("original_ID", "out_ID");
            joinedDF = joinedDF.withColumnRenamed("ID", "in_ID");
            edgeDFs.append(joinedDF);
        });

        ("gt_raw_data.gt_data", edgeDFs);
    }

    def getCLAL() = {    
        var edgeDFs = ListBuffer[DataFrame]();

        // CONTAINER INFO
        selectCols = Array("ID");
        var containerDF = getTableDF(dataModelKeySpace, "container", selectCols, true); 
        containerDF = containerDF.withColumnRenamed("ID", "out_ID");

        // LOCATION INFO
        selectCols = Array("original_ID");
        var locationDF = getTableDF(dataModelKeySpace, "location", selectCols, false); 
        locationDF = locationDF.na.drop(Array("original_ID"));
        locationDF = locationDF.withColumnRenamed("original_ID", "in_ID")

        // Join the tables based on ID
        var dfSubsets = subsetDataFrame(containerDF, "out_ID", largeTableSubsets);
        dfSubsets.foreach((subset) => {
            var joinedDF = subset.join(locationDF, col("out_ID") === col("in_ID"));
            edgeDFs.append(joinedDF);
        });

        ("gt_raw_data.gt_data", edgeDFs);
    }
    
    list += (RelDef("queried", "ip_address", "container") -> getIQC _);
    list += (RelDef("shipped", "vessel", "container") -> getIQC _);
    list += (RelDef("associated_with", "entity", "container") -> getPAWC _);
    list += (RelDef("located_at", "container", "location") -> getCLAL _);
    
    list;
}

def getCAEdgeDataFrames() = {
    var list = HashMap[RelDef, () => (String, ListBuffer[DataFrame])]();
    var keySpace = "ca_raw_data";
    var selectCols = Array[String]();

    def getCIC() = {
        var edgeDFs = ListBuffer[DataFrame]();
        selectCols = Array("ID");
        var df = getTableDF(dataModelKeySpace, "city", selectCols, true);
        df = df.filter(col("raw_source_parent_table") === lit("observations_btc_0"));
        df = df.withColumn("out_ID", col("ID"));
        df = df.withColumnRenamed("ID", "in_ID");
        edgeDFs.append(df);
        ("ca_raw_data.observations_btc_0", edgeDFs);
    }    

    def getLIC() = {
        var edgeDFs = ListBuffer[DataFrame]();
        selectCols = Array("ID");
        var df = getTableDF(dataModelKeySpace, "location", selectCols, true);
        df = df.filter(col("raw_source_parent_table") === lit("observations_btc_0"));
        df = df.withColumn("out_ID", col("ID"));
        df = df.withColumnRenamed("ID", "in_ID");
        edgeDFs.append(df);
        ("ca_raw_data.observations_btc_0", edgeDFs);
    }    

    def getCCWSIC() = {
        var edgeDFs = ListBuffer[DataFrame]();
        selectCols = Array("ID");
        var df = getTableDF(dataModelKeySpace, "cryptocurrency_wallet_software", selectCols, true);
        df = df.filter(col("raw_source_parent_table") === lit("observations_btc_0"));
        df = df.withColumn("out_ID", col("ID"));
        df = df.withColumnRenamed("ID", "in_ID");
        edgeDFs.append(df);
        ("ca_raw_data.observations_btc_0", edgeDFs);
    }    

    def getBNIC() = {
        var edgeDFs = ListBuffer[DataFrame]();
        selectCols = Array("ID");
        var df = getTableDF(dataModelKeySpace, "bitcoin_node", selectCols, true);
        df = df.filter(col("raw_source_parent_table") === lit("observations_btc_0"));
        df = df.withColumn("out_ID", col("ID"));
        df = df.withColumnRenamed("ID", "in_ID");
        edgeDFs.append(df);
        ("ca_raw_data.observations_btc_0", edgeDFs);
    }    

    def getIPIC() = {
        var edgeDFs = ListBuffer[DataFrame]();
        selectCols = Array("ID");
        var df = getTableDF(dataModelKeySpace, "ip_address", selectCols, true);
        df = df.filter(col("raw_source_parent_table") === lit("observations_btc_0"));
        df = df.withColumn("out_ID", col("ID"));
        df = df.withColumnRenamed("ID", "in_ID");
        edgeDFs.append(df);
        ("ca_raw_data.observations_btc_0", edgeDFs);
    }    

    def getPHRA() = {
        var edgeDFs = ListBuffer[DataFrame]();
        selectCols = Array("ID", "root_address");
        var df = getTableDF(keySpace, "clusters_btc_0", selectCols, true);
        df = df.withColumnRenamed("ID", "out_ID");
        var addrDF = getTableDF(keySpace, "addresses_btc_0", selectCols, false);
        addrDF = addrDF.withColumnRenamed("ID", "in_ID");
        addrDF = addrDF.withColumnRenamed("root_address", "child_root_address");
        df = df.join(addrDF, col("root_address") === col("child_root_address"));
        df = df.drop("child_root_address");
        edgeDFs.append(df);
        ("ca_raw_data.clusters_btc_0", edgeDFs);
    }    

    def getISHA() = {
        var edgeDFs = ListBuffer[DataFrame]();
        selectCols = Array("ID", "address");
        var df = getTableDF(dataModelKeySpace, "internet_source", selectCols, true);
        df = df.withColumnRenamed("ID", "out_ID");
        var addrDF = getTableDF(dataModelKeySpace, "cryptocurrency_address", selectCols, false);
        addrDF = addrDF.withColumnRenamed("ID", "in_ID");
        addrDF = addrDF.withColumnRenamed("address", "child_address");
        df = df.join(addrDF, col("address") === col("child_address"));
        df = df.drop("child_address");
        edgeDFs.append(df);
        ("ca_raw_data.osints_btc_0", edgeDFs);
    }    

    def getCWSHRACC() = {
        var edgeDFs = ListBuffer[DataFrame]();
        selectCols = Array("ID", "root_address");
        var df = getTableDF(dataModelKeySpace, "cryptocurrency_wallet_software", selectCols, true);
        df = df.withColumnRenamed("ID", "out_ID");
        var addrDF = getTableDF(keySpace, "addresses_btc_0", selectCols, false);
        addrDF = addrDF.withColumnRenamed("ID", "in_ID");
        addrDF = addrDF.withColumnRenamed("root_address", "child_root_address");
        df = df.join(addrDF, col("root_address") === col("child_root_address"));
        df = df.drop("child_root_address");
        edgeDFs.append(df);
        ("ca_raw_data.clusters_btc_0", edgeDFs);
    }    

    list += (RelDef("in", "city", "country") -> getCIC _);
    list += (RelDef("for", "country", "city") -> (() => reverseEdgesDirection(getCIC _)));

    list += (RelDef("in", "location", "city") -> getLIC _);
    list += (RelDef("for", "city", "location") -> (() => reverseEdgesDirection(getLIC _)));
    list += (RelDef("in", "location", "country") -> getLIC _);
    list += (RelDef("for", "country", "location") -> (() => reverseEdgesDirection(getLIC _)));

    list += (RelDef("at", "cryptocurrency_wallet_software", "location") -> getCCWSIC _);
    list += (RelDef("for", "location", "cryptocurrency_wallet_software") -> (() => reverseEdgesDirection(getCCWSIC _)));

    list += (RelDef("at", "bitcoin_node", "location") -> getBNIC _);
    list += (RelDef("for", "location", "bitcoin_node") -> (() => reverseEdgesDirection(getBNIC _)));

    list += (RelDef("at", "ip_address", "location") -> getIPIC _);
    list += (RelDef("for", "location", "ip_address") -> (() => reverseEdgesDirection(getIPIC _)));

    list += (RelDef("has_root_address", "entity", "cryptocurrency_address") -> getPHRA _);  
    list += (RelDef("has_root_address", "cryptocurrency_address", "entity") -> (() => reverseEdgesDirection(getPHRA _)));  

    list += (RelDef("has_address", "internet_source", "cryptocurrency_address") -> getISHA _);  
    list += (RelDef("has_address", "cryptocurrency_address", "internet_source") -> (() => reverseEdgesDirection(getISHA _)));  

    list += (RelDef("has_root_address", "cryptocurrency_wallet_software", "cryptocurrency_address") -> getCWSHRACC _);  
    list += (RelDef("has_root_address", "cryptocurrency_address", "cryptocurrency_wallet_software") -> (() => reverseEdgesDirection(getCWSHRACC _)));  

    list;
}

def getSDNEdgeDataFrames() = {
    var list = HashMap[RelDef, () => (String, ListBuffer[DataFrame])]();
    list;
}

def getGTIPAddresses():(String, ListBuffer[DataFrame]) = {
    var gtDFs = ListBuffer[DataFrame]();
    var keySpace = "gt_raw_data";
    var selectCols = Array[String]();

    selectCols = Array("ID", "IP_ADDRESS", "INQUIRY_DATETIME");
    var gtDF = getTableDF(keySpace, "gt_data", selectCols, true);

    gtDF = gtDF.filter(col("IP_ADDRESS") !== "");
    gtDF = gtDF.withColumnRenamed("IP_ADDRESS", "ip");
    gtDF = gtDF.withColumn("when_observed", to_timestamp(col("INQUIRY_DATETIME"), "dd/MM/yyyy HH:mm:ss"));
    gtDF = gtDF.drop("INQUIRY_DATETIME");

    gtDFs.append(gtDF);

    return ("gt_raw_data.gt_data", gtDFs);
}

def getGTVesselDataFrames():(String, ListBuffer[DataFrame]) = {
    var gtDFs = ListBuffer[DataFrame]();
    var keySpace = "gt_raw_data";
    var selectCols = Array[String]();

    selectCols = Array("ID", "VESSEL_NAME", "BILL_OF_LADING", "VESSEL_CALL_SIGN", "CARRIER", "VESSEL_IMO_NUMBER");
    var gtDF = getTableDF(keySpace, "gt_data", selectCols, true);
    
    // VESSEL data for VESSEL table
    gtDF = gtDF.filter(col("VESSEL_NAME") !== "");
    gtDF = gtDF.withColumnRenamed("VESSEL_NAME", "name");
    gtDF = gtDF.withColumnRenamed("BILL_OF_LADING", "bill_of_lading");
    gtDF = gtDF.withColumnRenamed("VESSEL_CALL_SIGN", "call_sign");
    gtDF = gtDF.withColumnRenamed("CARRIER", "carrier");
    gtDF = gtDF.withColumnRenamed("VESSEL_IMO_NUMBER", "imo_number");
    gtDFs.append(gtDF);
    
    return ("gt_raw_data.gt_data", gtDFs);
}

def getGTContainerDataFrames():(String, ListBuffer[DataFrame]) = {
    var gtDFs = ListBuffer[DataFrame]();
    var keySpace = "gt_raw_data";
    var selectCols = Array[String]();

    selectCols = Array("ID", "BILL_OF_LADING", "CONTAINER_NUMBER", "CARGO_DESCRIPTION");
    var gtDF = getTableDF(keySpace, "gt_data", selectCols, true); 
    
    // CONTAINER data for CONTAINER table
    gtDF = gtDF.filter(col("CONTAINER_NUMBER") !== "");
    gtDF = gtDF.withColumnRenamed("CONTAINER_NUMBER", "container_number");
    gtDF = gtDF.withColumnRenamed("BILL_OF_LADING", "bill_of_lading");
    gtDF = gtDF.withColumnRenamed("CARGO_DESCRIPTION", "cargo_description");
    gtDFs.append(gtDF);
    
    return ("gt_raw_data.gt_data", gtDFs);
}

def getGTLocationDataFrames():(String, ListBuffer[DataFrame]) = {
    var gtDFs = ListBuffer[DataFrame]();
    var keySpace = "gt_raw_data";
    var selectCols = Array[String]();

    var columnsUsed:Array[Array[String]] = Array(
        Array("ID", "DELIVERY_CITY", "DELIVERY_COUNTRY", "DELIVERY_LOC_ID", "DELIVERY_LRT_CD", "DELIVERY_RAW_INFO"),
        Array("ID", "DESTINATION_CITY", "DESTINATION_COUNTRY", "DESTINATION_LOC_ID", "DESTINATION_LRT_CD", "DESTINATION_RAW_INFO"),
        Array("ID", "DISCHARGE_PORT_CITY", "DISCHARGE_PORT_COUNTRY", "DISCHARGE_PORT_LOC_ID", "DISCHARGE_PORT_LRT_CD", "DISCHARGE_PORT_RAW_INFO"),
        Array("ID", "IN_GATE_CITY", "IN_GATE_COUNTRY", "IN_GATE_LOC_ID", "IN_GATE_LRT_CD", "IN_GATE_RAW_INFO"),
        Array("ID", "LOAD_PORT_CITY", "LOAD_PORT_COUNTRY", "LOAD_PORT_LOC_ID", "LOAD_PORT_LRT_CD", "LOAD_PORT_RAW_INFO"),
        Array("ID", "OTHER_PORT_1_CITY", "OTHER_PORT_1_COUNTRY", "OTHER_PORT_1_LOC_ID", "OTHER_PORT_1_LRT_CD", "OTHER_PORT_1_RAW_INFO"),
        Array("ID", "OTHER_PORT_2_CITY", "OTHER_PORT_2_COUNTRY", "OTHER_PORT_2_LOC_ID", "OTHER_PORT_2_LRT_CD", "OTHER_PORT_2_RAW_INFO"),
        Array("ID", "OTHER_PORT_3_CITY", "OTHER_PORT_3_COUNTRY", "OTHER_PORT_3_LOC_ID", "OTHER_PORT_3_LRT_CD", "OTHER_PORT_3_RAW_INFO"),
        Array("ID", "OUT_GATE_CITY", "OUT_GATE_COUNTRY", "OUT_GATE_LOC_ID", "OUT_GATE_LRT_CD", "OUT_GATE_RAW_INFO"),
        Array("ID", "PLACE_OF_RECEIPT_CITY", "PLACE_OF_RECEIPT_COUNTRY", "PLACE_OF_RECEIPT_LOC_ID", "PLACE_OF_RECEIPT_LRT_CD", "PLACE_OF_RECEIPT_RAW_INFO")
    );
    selectCols = getUniqueColNames(columnsUsed);

    var tableDF = getTableDF(keySpace, "gt_data", selectCols, true);

    // DELIVERY data for LOCATION table
    var gtDF = tableDF.select("ID", "DELIVERY_CITY", "DELIVERY_COUNTRY", "DELIVERY_RAW_INFO");
    gtDF = gtDF.filter(col("DELIVERY_COUNTRY") !== "");
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_delivery"))));
    gtDF = gtDF.withColumnRenamed("DELIVERY_COUNTRY", "country_code");
    gtDF = gtDF.withColumnRenamed("DELIVERY_CITY", "city");
    gtDF = gtDF.withColumnRenamed("DELIVERY_RAW_INFO", "raw_info");
    gtDF = gtDF.withColumn("location_type", lit("Delivery"));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDFs.append(gtDF);
    // DESTINATION data for LOCATION table
    gtDF = tableDF.select("ID", "DESTINATION_CITY", "DESTINATION_COUNTRY", "DESTINATION_RAW_INFO");
    gtDF = gtDF.filter(col("DESTINATION_COUNTRY") !== "");
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_destination"))));
    gtDF = gtDF.withColumnRenamed("DESTINATION_COUNTRY", "country_code");
    gtDF = gtDF.withColumnRenamed("DESTINATION_CITY", "city");
    gtDF = gtDF.withColumnRenamed("DESTINATION_RAW_INFO", "raw_info");
    gtDF = gtDF.withColumn("location_type", lit("Destination"));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDFs.append(gtDF);
    // DISCHARGE PORT data for LOCATION table
    gtDF = tableDF.select("ID", "DISCHARGE_PORT_CITY", "DISCHARGE_PORT_COUNTRY", "DISCHARGE_PORT_RAW_INFO");
    gtDF = gtDF.filter(col("DISCHARGE_PORT_COUNTRY") !== "");
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_dischargeport"))));
    gtDF = gtDF.withColumnRenamed("DISCHARGE_PORT_COUNTRY", "country_code");
    gtDF = gtDF.withColumnRenamed("DISCHARGE_PORT_CITY", "city");
    gtDF = gtDF.withColumnRenamed("DISCHARGE_PORT_RAW_INFO", "raw_info");
    gtDF = gtDF.withColumn("location_type", lit("Discharge Port"));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDFs.append(gtDF);    
    // IN GATE data for LOCATION table
    gtDF = tableDF.select("ID", "IN_GATE_CITY", "IN_GATE_COUNTRY", "IN_GATE_RAW_INFO");
    gtDF = gtDF.filter(col("IN_GATE_COUNTRY") !== "");
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_ingate"))));
    gtDF = gtDF.withColumnRenamed("IN_GATE_COUNTRY", "country_code");
    gtDF = gtDF.withColumnRenamed("IN_GATE_CITY", "city");
    gtDF = gtDF.withColumnRenamed("IN_GATE_RAW_INFO", "raw_info");
    gtDF = gtDF.withColumn("location_type", lit("In Gate"));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDFs.append(gtDF);
    // LOAD PORT data for LOCATION table
    gtDF = tableDF.select("ID", "LOAD_PORT_CITY", "LOAD_PORT_COUNTRY", "LOAD_PORT_RAW_INFO");
    gtDF = gtDF.filter(col("LOAD_PORT_COUNTRY") !== "");
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_loadport"))));
    gtDF = gtDF.withColumnRenamed("LOAD_PORT_COUNTRY", "country_code");
    gtDF = gtDF.withColumnRenamed("LOAD_PORT_CITY", "city");
    gtDF = gtDF.withColumnRenamed("LOAD_PORT_RAW_INFO", "raw_info");
    gtDF = gtDF.withColumn("location_type", lit("Load Port"));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDFs.append(gtDF);
    // OTHER PORT 1 data for LOCATION table
    gtDF = tableDF.select("ID", "OTHER_PORT_1_CITY", "OTHER_PORT_1_COUNTRY", "OTHER_PORT_1_RAW_INFO");
    gtDF = gtDF.filter(col("OTHER_PORT_1_COUNTRY") !== "");
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_otherport1"))));
    gtDF = gtDF.withColumnRenamed("OTHER_PORT_1_COUNTRY", "country_code");
    gtDF = gtDF.withColumnRenamed("OTHER_PORT_1_CITY", "city");
    gtDF = gtDF.withColumnRenamed("OTHER_PORT_1_LOC_ID", "location_id");
    gtDF = gtDF.withColumnRenamed("OTHER_PORT_1_LRT_CD", "location_reference_type");
    gtDF = gtDF.withColumnRenamed("OTHER_PORT_1_RAW_INFO", "raw_info");
    gtDF = gtDF.withColumn("location_type", lit("Other Port 1"));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDFs.append(gtDF);
    // OTHER PORT 2 data for LOCATION table
    gtDF = tableDF.select("ID", "OTHER_PORT_2_CITY", "OTHER_PORT_2_COUNTRY", "OTHER_PORT_2_RAW_INFO");
    gtDF = gtDF.filter(col("OTHER_PORT_2_COUNTRY") !== "");
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_otherport2"))));
    gtDF = gtDF.withColumnRenamed("OTHER_PORT_2_COUNTRY", "country_code");
    gtDF = gtDF.withColumnRenamed("OTHER_PORT_2_CITY", "city");
    gtDF = gtDF.withColumnRenamed("OTHER_PORT_2_RAW_INFO", "raw_info");
    gtDF = gtDF.withColumn("location_type", lit("Other Port 2"));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDFs.append(gtDF);
    // OTHER PORT 3 data for LOCATION table
    gtDF = tableDF.select("ID", "OTHER_PORT_3_CITY", "OTHER_PORT_3_COUNTRY", "OTHER_PORT_3_RAW_INFO");
    gtDF = gtDF.filter(col("OTHER_PORT_3_COUNTRY") !== "");
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_otherport3"))));
    gtDF = gtDF.withColumnRenamed("OTHER_PORT_3_COUNTRY", "country_code");
    gtDF = gtDF.withColumnRenamed("OTHER_PORT_3_CITY", "city");
    gtDF = gtDF.withColumnRenamed("OTHER_PORT_3_RAW_INFO", "raw_info");
    gtDF = gtDF.withColumn("location_type", lit("Other Port 3"));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDFs.append(gtDF);
    // OUT GATE data for LOCATION table
    gtDF = tableDF.select("ID", "OUT_GATE_CITY", "OUT_GATE_COUNTRY", "OUT_GATE_RAW_INFO");
    gtDF = gtDF.filter(col("OUT_GATE_COUNTRY") !== "");
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_outgate"))));
    gtDF = gtDF.withColumnRenamed("OUT_GATE_COUNTRY", "country_code");
    gtDF = gtDF.withColumnRenamed("OUT_GATE_CITY", "city");
    gtDF = gtDF.withColumnRenamed("OUT_GATE_RAW_INFO", "raw_info");
    gtDF = gtDF.withColumn("location_type", lit("Out Gate"));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDFs.append(gtDF);
    // PLACE OF RECEIPT data for LOCATION table
    gtDF = tableDF.select("ID", "PLACE_OF_RECEIPT_CITY", "PLACE_OF_RECEIPT_COUNTRY", "PLACE_OF_RECEIPT_RAW_INFO");
    gtDF = gtDF.filter(col("PLACE_OF_RECEIPT_COUNTRY") !== "");
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_placeofreceipt"))));
    gtDF = gtDF.withColumnRenamed("PLACE_OF_RECEIPT_COUNTRY", "countr_code");
    gtDF = gtDF.withColumnRenamed("PLACE_OF_RECEIPT_CITY", "city");
    gtDF = gtDF.withColumnRenamed("PLACE_OF_RECEIPT_RAW_INFO", "raw_info");
    gtDF = gtDF.withColumn("location_type", lit("Place of Receipt"));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDFs.append(gtDF);

    return ("gt_raw_data.gt_data", gtDFs);
}

def getGTEntityDataFrames():(String, ListBuffer[DataFrame]) = {
    var gtDFs = ListBuffer[DataFrame]();
    var keySpace = "gt_raw_data";
    var selectCols = Array[String]();

    def isOrganization(s:String) = {
        var str = s.toUpperCase();
        var lookFor = ListBuffer[String]();
        lookFor.appendAll(Array("CO.", "LTD", "LLC", "INC", "LOGISTICS", "&"));
        lookFor.appendAll(Array("GMBH", "OVERSEAS", "TRADING", "LIMITED", "INTERNATIONAL", "INTL", "RESEARCH"));
        lookFor.appendAll(Array("GLOBAL", "INDUSTRY", "INDUSTRIES", "DEVELOPMENT", "CORPORATION"));
        lookFor.appendAll(Array("FORWARDING", "MANAGEMENT", "INDUSTRIES", "DEVELOPMENT", "CORPORATION"));
        lookFor.appendAll(Array("CAPITAL", "FINANCIAL", "ADVISOR", "INVESTMENT", "GROUP", "ASSET", "PARTNERS"));
        lookFor.appendAll(Array("SERVICE", "ASSOCIATES", "PLANNING", "STRATEGIES", "SOLUTIONS", "PORTFOLIO", "STREET"));
        lookFor.appendAll(Array("TRUST", "COMPLIANCE", "SUPPLY", "DISTRIBUTION", "PAPER", "S.A.", "NATIONAL"));

        lookFor.appendAll(Array("INGREDIENTS"));

        var flag = false;
        lookFor.foreach((s) => {
            flag = flag || str.contains(s);
        });
        flag;
    }

    def isPerson(s:String) = {
        val flag = !isOrganization(s);
        flag;
    }

    def getFirstLastName(s:String) = {
        var first = "";
        var last = "";
        if (isOrganization(s)) {

        } else {
            if (s.contains(",")) {
                val arr = s.split(",", 2);
                last = arr(0).trim();
                first = arr(1).trim();
            } else if (s.contains(" ")) {
                val arr = s.split(" ", 2);
                first = arr(0).trim();
                last = arr(1).trim();
            } else {
                last = s;
            }
        }
        (first, last);
    }

    val isOrganizationUDF = udf((s:String) => {
        isOrganization(s);
    });

    val isPersonUDF = udf((s:String) => {
        isPerson(s);
    });

    val firstNameUDF = udf((s:String) => {
        val (first, last) = getFirstLastName(s);
        first;
    });

    val lastNameUDF = udf((s:String) => {
        val (first, last) = getFirstLastName(s);
        last;
    });

    var columnsUsed:Array[Array[String]] = Array(
        Array("ID", "AGENT_NAME", "AGENT_ADDRESS_1", "AGENT_USP", "AGENT_CODE"),
        Array("ID", "BOOKING_PARTY_NAME", "BOOKING_ADDRESS_1", "BOOKING_PARTY_USP", "BOOKING_PARTY_CODE", "BOOKING_CONTACT_NAME"),
        Array("ID", "CONSIGNEE_NAME", "CONSIGNEE_ADDRESS_1", "CONSIGNEE_USP", "CONSIGNEE_CODE", "CONSIGNEE_CONTACT_NAME"),
        Array("ID", "FORWARDER_NAME", "FORWARDER_ADDRESS_1", "FORWARDER_USP", "FORWARDER_CODE", "FORWARDER_CONTACT_NAME"),
        Array("ID", "NOTIFY_PARTY_NAME", "NOTIFY_PARTY_ADDRESS_1", "NOTIFY_PARTY_USP", "NOTIFY_PARTY_CODE", "NOTIFY_PARTY_CONTACT_NAME"),
        Array("ID", "NOTIFY_PARTY_2_NAME", "NOTIFY_PARTY_2_ADDRESS_1", "NOTIFY_PARTY_2_USP", "NOTIFY_PARTY_2_CODE", "NOTIFY_PARTY_2_CONTACT_NAME"),
        Array("ID", "OTHER_IP_NAME", "OTHER_IP_ADDRESS_1", "OTHER_IP_USP", "OTHER_IP_CONTACT_NAME"),
        Array("ID", "PAYOR_NAME", "PAYOR_ADDRESS_1", "PAYOR_USP", "PAYOR_CODE", "PAYOR_CONTACT_NAME"),
        Array("ID", "SHIPPER_NAME", "SHIPPER_ADDRESS_1", "SHIPPER_USP", "SHIPPER_CODE", "SHIPPER_CONTACT_NAME", "SHIPPER_OWNED_CONTAINER")
    );
    selectCols = getUniqueColNames(columnsUsed);

    // PULL DATA FROM 9 DIFFERENT "PARTY" ENTITIES WITHIN GT AND STORE AS DATAFRAME
    var df = getTableDF(keySpace, "gt_data", selectCols, true); 
    
    // AGENT data for PARTY table
    var gtDF = df.select("ID", "AGENT_NAME", "AGENT_ADDRESS_1");
    gtDF = gtDF.filter(col("AGENT_NAME") !== "");
    gtDF = gtDF.withColumnRenamed("AGENT_NAME", "name");
    gtDF = gtDF.withColumnRenamed("AGENT_ADDRESS_1", "address");
    gtDF = gtDF.withColumn("type", lit("Agent"));
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_agent"))));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDF = gtDF.withColumn("is_person", isPersonUDF(col("name")));
    gtDF = gtDF.withColumn("is_organization", isOrganizationUDF(col("name")));
    gtDF = gtDF.withColumn("is_person_probability", lit(0.5f));
    gtDF = gtDF.withColumn("is_organization_probability", lit(0.9f));
    gtDF = gtDF.withColumn("first_name", firstNameUDF(col("name")));
    gtDF = gtDF.withColumn("last_name", lastNameUDF(col("name")));
    gtDFs.append(gtDF);
    // BOOKING PARTY data for PARTY table
    gtDF = df.select("ID", "BOOKING_PARTY_NAME", "BOOKING_ADDRESS_1", "BOOKING_CONTACT_NAME");
    gtDF = gtDF.filter(col("BOOKING_PARTY_NAME") !== "");
    gtDF = gtDF.withColumnRenamed("BOOKING_PARTY_NAME", "name");
    gtDF = gtDF.withColumnRenamed("BOOKING_ADDRESS_1", "address");
    gtDF = gtDF.withColumnRenamed("BOOKING_CONTACT_NAME", "contact_name");
    gtDF = gtDF.withColumn("type", lit("Booking Entity"));
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_bookingentity"))));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDF = gtDF.withColumn("is_person", isPersonUDF(col("name")));
    gtDF = gtDF.withColumn("is_organization", isOrganizationUDF(col("name")));
    gtDF = gtDF.withColumn("is_person_probability", lit(0.5f));
    gtDF = gtDF.withColumn("is_organization_probability", lit(0.9f));
    gtDF = gtDF.withColumn("first_name", firstNameUDF(col("name")));
    gtDF = gtDF.withColumn("last_name", lastNameUDF(col("name")));
    gtDFs.append(gtDF);
    // CONSIGNEE data for PARTY table
    gtDF = df.select("ID", "CONSIGNEE_NAME", "CONSIGNEE_ADDRESS_1", "CONSIGNEE_CONTACT_NAME");
    gtDF = gtDF.filter(col("CONSIGNEE_NAME") !== "");
    gtDF = gtDF.withColumnRenamed("CONSIGNEE_NAME", "name");
    gtDF = gtDF.withColumnRenamed("CONSIGNEE_ADDRESS_1", "address");
    gtDF = gtDF.withColumnRenamed("CONSIGNEE_CONTACT_NAME", "contact_name");
    gtDF = gtDF.withColumn("type", lit("Consignee"));
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_consignee"))));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDF = gtDF.withColumn("is_person", isPersonUDF(col("name")));
    gtDF = gtDF.withColumn("is_organization", isOrganizationUDF(col("name")));
    gtDF = gtDF.withColumn("is_person_probability", lit(0.5f));
    gtDF = gtDF.withColumn("is_organization_probability", lit(0.9f));
    gtDF = gtDF.withColumn("first_name", firstNameUDF(col("name")));
    gtDF = gtDF.withColumn("last_name", lastNameUDF(col("name")));
    gtDFs.append(gtDF);
    // FORWARDER data for PARTY table
    gtDF = df.select("ID", "FORWARDER_NAME", "FORWARDER_ADDRESS_1", "FORWARDER_CONTACT_NAME");
    gtDF = gtDF.filter(col("FORWARDER_NAME") !== "");
    gtDF = gtDF.withColumnRenamed("FORWARDER_NAME", "name");
    gtDF = gtDF.withColumnRenamed("FORWARDER_ADDRESS_1", "address");
    gtDF = gtDF.withColumnRenamed("FORWARDER_CONTACT_NAME", "contact_name");
    gtDF = gtDF.withColumn("type", lit("Forwarder"));
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_forwarder"))));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDF = gtDF.withColumn("is_person", isPersonUDF(col("name")));
    gtDF = gtDF.withColumn("is_organization", isOrganizationUDF(col("name")));
    gtDF = gtDF.withColumn("is_person_probability", lit(0.5f));
    gtDF = gtDF.withColumn("is_organization_probability", lit(0.9f));
    gtDF = gtDF.withColumn("first_name", firstNameUDF(col("name")));
    gtDF = gtDF.withColumn("last_name", lastNameUDF(col("name")));
    gtDFs.append(gtDF);
    // NOTIFY PARTY data for PARTY table
    gtDF = df.select("ID", "NOTIFY_PARTY_NAME", "NOTIFY_PARTY_ADDRESS_1", "NOTIFY_PARTY_CONTACT_NAME");
    gtDF = gtDF.filter(col("NOTIFY_PARTY_NAME") !== "");
    gtDF = gtDF.withColumnRenamed("NOTIFY_PARTY_NAME", "name");
    gtDF = gtDF.withColumnRenamed("NOTIFY_PARTY_ADDRESS_1", "address");
    gtDF = gtDF.withColumnRenamed("NOTIFY_PARTY_CONTACT_NAME", "contact_name");
    gtDF = gtDF.withColumn("type", lit("Notify Entity"));
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_notifyentity"))));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDF = gtDF.withColumn("is_person", isPersonUDF(col("name")));
    gtDF = gtDF.withColumn("is_organization", isOrganizationUDF(col("name")));
    gtDF = gtDF.withColumn("is_person_probability", lit(0.5f));
    gtDF = gtDF.withColumn("is_organization_probability", lit(0.9f));
    gtDF = gtDF.withColumn("first_name", firstNameUDF(col("name")));
    gtDF = gtDF.withColumn("last_name", lastNameUDF(col("name")));
    gtDFs.append(gtDF);
    // NOTIFY PARTY 2 data for PARTY table
    gtDF = df.select("ID", "NOTIFY_PARTY_2_NAME", "NOTIFY_PARTY_2_ADDRESS_1", "NOTIFY_PARTY_2_CONTACT_NAME");
    gtDF = gtDF.filter(col("NOTIFY_PARTY_2_NAME") !== "");
    gtDF = gtDF.withColumnRenamed("NOTIFY_PARTY_2_NAME", "name");
    gtDF = gtDF.withColumnRenamed("NOTIFY_PARTY_2_ADDRESS_1", "address");
    gtDF = gtDF.withColumnRenamed("NOTIFY_PARTY_2_CONTACT_NAME", "contact_name");
    gtDF = gtDF.withColumn("type", lit("Notify Entity 2"));
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_notifyentity2"))));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDF = gtDF.withColumn("is_person", isPersonUDF(col("name")));
    gtDF = gtDF.withColumn("is_organization", isOrganizationUDF(col("name")));
    gtDF = gtDF.withColumn("is_person_probability", lit(0.5f));
    gtDF = gtDF.withColumn("is_organization_probability", lit(0.9f));
    gtDF = gtDF.withColumn("first_name", firstNameUDF(col("name")));
    gtDF = gtDF.withColumn("last_name", lastNameUDF(col("name")));
    gtDFs.append(gtDF);
    // OTHER IP data for PARTY table
    gtDF = df.select("ID", "OTHER_IP_NAME", "OTHER_IP_ADDRESS_1", "OTHER_IP_CONTACT_NAME");
    gtDF = gtDF.filter(col("OTHER_IP_NAME") !== "");
    gtDF = gtDF.withColumnRenamed("OTHER_IP_NAME", "name");
    gtDF = gtDF.withColumnRenamed("OTHER_IP_ADDRESS_1", "address");
    gtDF = gtDF.withColumnRenamed("OTHER_IP_CONTACT_NAME", "contact_name");
    gtDF = gtDF.withColumn("type", lit("Other IP"));
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_otherip"))));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDF = gtDF.withColumn("is_person", isPersonUDF(col("name")));
    gtDF = gtDF.withColumn("is_organization", isOrganizationUDF(col("name")));
    gtDF = gtDF.withColumn("is_person_probability", lit(0.5f));
    gtDF = gtDF.withColumn("is_organization_probability", lit(0.9f));
    gtDF = gtDF.withColumn("first_name", firstNameUDF(col("name")));
    gtDF = gtDF.withColumn("last_name", lastNameUDF(col("name")));
    gtDFs.append(gtDF);
    // PAYOR data for PARTY table
    gtDF = df.select("ID", "PAYOR_NAME", "PAYOR_ADDRESS_1", "PAYOR_CONTACT_NAME");
    gtDF = gtDF.filter(col("PAYOR_NAME") !== "");
    gtDF = gtDF.withColumnRenamed("PAYOR_NAME", "name");
    gtDF = gtDF.withColumnRenamed("PAYOR_ADDRESS_1", "address");
    gtDF = gtDF.withColumnRenamed("PAYOR_CONTACT_NAME", "contact_name");
    gtDF = gtDF.withColumn("type", lit("Payor"));
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_payor"))));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDF = gtDF.withColumn("is_person", isPersonUDF(col("name")));
    gtDF = gtDF.withColumn("is_organization", isOrganizationUDF(col("name")));
    gtDF = gtDF.withColumn("is_person_probability", lit(0.5f));
    gtDF = gtDF.withColumn("is_organization_probability", lit(0.9f));
    gtDF = gtDF.withColumn("first_name", firstNameUDF(col("name")));
    gtDF = gtDF.withColumn("last_name", lastNameUDF(col("name")));
    gtDFs.append(gtDF);
    // SHIPPER data for PARTY table
    gtDF = df.select("ID", "SHIPPER_NAME", "SHIPPER_ADDRESS_1", "SHIPPER_CONTACT_NAME");
    gtDF = gtDF.filter(col("SHIPPER_NAME") !== "");
    gtDF = gtDF.withColumnRenamed("SHIPPER_NAME", "name");
    gtDF = gtDF.withColumnRenamed("SHIPPER_ADDRESS_1", "address");
    gtDF = gtDF.withColumnRenamed("SHIPPER_CONTACT_NAME", "contact_name");
    gtDF = gtDF.withColumn("type", lit("Shipper"));
    gtDF = gtDF.withColumn("New_ID", uuidUDF(concat(col("ID"), lit("_shipper"))));
    gtDF = gtDF.withColumnRenamed("ID", "original_id");
    gtDF = gtDF.withColumnRenamed("New_ID", "ID");
    gtDF = gtDF.withColumn("is_person", isPersonUDF(col("name")));
    gtDF = gtDF.withColumn("is_organization", isOrganizationUDF(col("name")));
    gtDF = gtDF.withColumn("is_person_probability", lit(0.5f));
    gtDF = gtDF.withColumn("is_organization_probability", lit(0.9f));
    gtDF = gtDF.withColumn("first_name", firstNameUDF(col("name")));
    gtDF = gtDF.withColumn("last_name", lastNameUDF(col("name")));
    gtDFs.append(gtDF);
    
    return ("gt_raw_data.gt_data", gtDFs);
}

def getCACountryDataFrames():(String, ListBuffer[DataFrame]) = {
    var caDFs = ListBuffer[DataFrame]();
    var keySpace = "ca_raw_data";
    var selectCols = Array[String]();
    
    // load the cluster table to DF > DF to company data model
    selectCols = Array("ID", "country");
    var caRawObservationTableDF = getTableDF(keySpace, "observations_btc_0", selectCols, true); 
    caRawObservationTableDF = caRawObservationTableDF.withColumnRenamed("country", "name");
    
    caDFs.append(caRawObservationTableDF);
    
    return ("ca_raw_data.observations_btc_0", caDFs);
}

def getCACityDataFrames():(String, ListBuffer[DataFrame]) = {
    var caDFs = ListBuffer[DataFrame]();
    var keySpace = "ca_raw_data";
    var selectCols = Array[String]();
    
    // load the cluster table to DF > DF to company data model
    selectCols = Array("ID", "city");
    var caRawObservationTableDF = getTableDF(keySpace, "observations_btc_0", selectCols, true); 
    caRawObservationTableDF = caRawObservationTableDF.withColumnRenamed("city", "name");
    
    caDFs.append(caRawObservationTableDF);
    
    return ("ca_raw_data.observations_btc_0", caDFs);
}

def getCALocationDataFrames():(String, ListBuffer[DataFrame]) = {
    var caDFs = ListBuffer[DataFrame]();
    var keySpace = "ca_raw_data";
    var selectCols = Array[String]();
    
    // load the cluster table to DF > DF to company data model
    selectCols = Array("ID", "ip", "long", "lat");
    var caRawObservationTableDF = getTableDF(keySpace, "observations_btc_0", selectCols, true);
    
    caDFs.append(caRawObservationTableDF);
    
    return ("ca_raw_data.observations_btc_0", caDFs);
}

def getCACryptoCurrencyWalletSoftwareDataFrames():(String, ListBuffer[DataFrame]) = {
    var caDFs = ListBuffer[DataFrame]();
    var keySpace = "ca_raw_data";
    var selectCols = Array[String]();
    
    // load the cluster table to DF > DF to company data model
    selectCols = Array("ID", "ip", "root_address", "port", "subversion", "connection_time", "observation_time", "anonymous");
    var caRawObservationTableDF = getTableDF(keySpace, "observations_btc_0", selectCols, true); 

    caDFs.append(caRawObservationTableDF);
    
    return ("ca_raw_data.observations_btc_0", caDFs);
}

def getCABitcoinNodeDataFrames():(String, ListBuffer[DataFrame]) = {
    var caDFs = ListBuffer[DataFrame]();
    var keySpace = "ca_raw_data";
    var selectCols = Array[String]();
    
    // load the cluster table to DF > DF to company data model
    selectCols = Array("ID", "ip", "isp", "anonymous");
    var caRawObservationTableDF = getTableDF(keySpace, "observations_btc_0", selectCols, true); 
    
    caDFs.append(caRawObservationTableDF);
    
    return ("ca_raw_data.observations_btc_0", caDFs);
}

def getCAIPAddressDataFrames():(String, ListBuffer[DataFrame]) = {
    var caDFs = ListBuffer[DataFrame]();
    var keySpace = "ca_raw_data";
    var selectCols = Array[String]();
    
    // load the cluster table to DF > DF to company data model
    selectCols = Array("ID", "ip", "isp", "anonymous");
    var caRawObservationTableDF = getTableDF(keySpace, "observations_btc_0", selectCols, true); 
    
    caDFs.append(caRawObservationTableDF);
    
    return ("ca_raw_data.observations_btc_0", caDFs);
}

def getCAInternetSourceDataFrames():(String, ListBuffer[DataFrame]) = {
    var caDFs = ListBuffer[DataFrame]();
    var keySpace = "ca_raw_data";
    var selectCols = Array[String]();
    
    // load the cluster table to DF > DF to company data model
    selectCols = Array("ID", "address", "label", "text", "url", "time", "category", "categorygroup");
    var caRawOsintTableDF = getTableDF(keySpace, "osints_btc_0", selectCols, true); 

    caDFs.append(caRawOsintTableDF);
    
    return ("ca_raw_data.osints_btc_0", caDFs);
}

def getCAAccountDataFrames():(String, ListBuffer[DataFrame]) = {
    var caDFs = ListBuffer[DataFrame]();
    var keySpace = "ca_raw_data";
    var selectCols = Array[String]();
    
    // load the cluster table to DF > DF to company data model
    selectCols = Array("ID", "root_address", "first_main", "last_main", "sent", "received", "fees", "sent_usd", "received_usd", "fees_usd", "deposits", "withdrawals");
    var caRawAccountTableDF = getTableDF(keySpace, "clusters_btc_0", selectCols, true); 
    
    caDFs.append(caRawAccountTableDF);
    
    return ("ca_raw_data.clusters_btc_0", caDFs);
}

def getCACryptoCurrencyAddressDataFrames():(String, ListBuffer[DataFrame]) = {
    var caDFs = ListBuffer[DataFrame]();
    var keySpace = "ca_raw_data";
    var selectCols = Array[String]();

    // load the cluster table to DF > DF to company data model
    selectCols = Array("ID", "address", "root_address");
    var caRawAddressTableDF = getTableDF(keySpace, "addresses_btc_0", selectCols, true); 

    caDFs.append(caRawAddressTableDF);
    
    return ("ca_raw_data.addresses_btc_0", caDFs);
}

def getCAEntityDataFrames():(String, ListBuffer[DataFrame]) = {
    var caDFs = ListBuffer[DataFrame]();
    var keySpace = "ca_raw_data";
    var selectCols = Array[String]();

    selectCols = Array("ID", "name", "root_address", "category");
    var clustersDF = getTableDF(keySpace, "clusters_btc_0", selectCols, true);

    // load the cluster table to DF > DF to company data model
    clustersDF = clustersDF.withColumnRenamed("root_address", "blockchain_root_address").withColumnRenamed("category", "blockchain_category");

    clustersDF = clustersDF.withColumn("is_person", lit(false));
    clustersDF = clustersDF.withColumn("is_organization", lit(true));
    clustersDF = clustersDF.withColumn("is_person_probability", lit(0.9f));
    clustersDF = clustersDF.withColumn("is_organization_probability", lit(0.9f));

    caDFs.append(clustersDF);
    
    return ("ca_raw_data.clusters_btc_0", caDFs);
}

def getZSAIPAddressDataFrames():(String, ListBuffer[DataFrame]) = {
    var dfs = ListBuffer[DataFrame]();
    var keySpace = "zsa_raw_data";
    var selectCols = Array[String]();

    selectCols = Array("ID", "MainIPAddressTimeStampText", "IPAddressText");
    var zsaRawMainIPAddressTableDF = getTableDF(keySpace, "MainIPAddress", selectCols, true); 
    zsaRawMainIPAddressTableDF = zsaRawMainIPAddressTableDF.withColumn("when_observed", to_timestamp(col("MainIPAddressTimeStampText")));
    zsaRawMainIPAddressTableDF = zsaRawMainIPAddressTableDF.drop("MainIPAddressTimeStampText");
    zsaRawMainIPAddressTableDF = zsaRawMainIPAddressTableDF.withColumnRenamed("IPAddressText", "ip");

    dfs.append(zsaRawMainIPAddressTableDF);

    return ("zsa_raw_data.mainipaddress", dfs);
}

def getZSAObjectDataFrames():(String, ListBuffer[DataFrame]) = {
    var zsaDFs = ListBuffer[DataFrame]();
    var keySpace = "zsa_raw_data";
    var selectCols = Array[String]();
    
    //load the parent raw table
    selectCols = Array("ID", "MainID", "ObjectID", "ObjectSubtypeCodeDescription", "ObjectTypeCodeDescription");
    var zsaRawObjectsTableDF = getTableDF(keySpace, "Objects", selectCols, true); 
    zsaRawObjectsTableDF = zsaRawObjectsTableDF.withColumnRenamed("MainID", "ObjectMainID");
    zsaRawObjectsTableDF = zsaRawObjectsTableDF.withColumnRenamed("ObjectSubtypeCodeDescription", "subtype");
    zsaRawObjectsTableDF = zsaRawObjectsTableDF.withColumnRenamed("ObjectTypeCodeDescription", "type");

    //load the object attributes child table
    selectCols = Array("MainID", "ObjectAttributeDescriptionText", "ObjectAttributeTypeCodeDescription");
    var zsaRawAObjectAttributesTableDF = getTableDF(keySpace, "ObjectsAttribute", selectCols, false, true, "MainID");
    zsaRawAObjectAttributesTableDF = zsaRawAObjectAttributesTableDF.withColumnRenamed("MainID", "ObjectAttributeMainID");
    zsaRawAObjectAttributesTableDF = zsaRawAObjectAttributesTableDF.withColumnRenamed("ObjectAttributeDescriptionText", "attribute_description");
    zsaRawAObjectAttributesTableDF = zsaRawAObjectAttributesTableDF.withColumnRenamed("ObjectAttributeTypeCodeDescription", "attribute_type");

    //join the tables based on AccountID
    var dfSubsets = subsetDataFrame(zsaRawObjectsTableDF, "ID", largeTableSubsets);
    dfSubsets.foreach((subset) => {
        var joinedDF = subset.join(zsaRawAObjectAttributesTableDF, col("ObjectMainID") === col("ObjectAttributeMainID"), "left_outer");
        joinedDF = joinedDF.select("ID", "subtype", "type", "attribute_description", "attribute_type");
        zsaDFs.append(joinedDF);
    });

    return ("zsa_raw_data.objects", zsaDFs);
}

def getZSAAccountDataFrames():(String, ListBuffer[DataFrame]) = {
    var zsaDFs = ListBuffer[DataFrame]();
    var keySpace = "zsa_raw_data";
    var selectCols = Array[String]();
    
    //load the parent raw table
    selectCols = Array("ID", "AccountID", "AccountMaximumValueAmount", "AccountNumberText", "AccountTypeCodeDescription", "OtherAccountTypeText");
    var zsaRawAccountTableDF = getTableDF(keySpace, "Account", selectCols, true); 
    zsaRawAccountTableDF = zsaRawAccountTableDF.withColumnRenamed("AccountMaximumValueAmount", "max_value_amount");
    zsaRawAccountTableDF = zsaRawAccountTableDF.withColumnRenamed("AccountNumberText", "number");
    zsaRawAccountTableDF = zsaRawAccountTableDF.withColumnRenamed("AccountTypeCodeDescription", "type");
    zsaRawAccountTableDF = zsaRawAccountTableDF.withColumnRenamed("OtherAccountTypeText", "other_account_type");
    zsaRawAccountTableDF = zsaRawAccountTableDF.select("ID", "max_value_amount", "number", "type", "other_account_type");

    zsaDFs.append(zsaRawAccountTableDF);
    
    return ("zsa_raw_data.account", zsaDFs);
}

def getZSANarrativeDataFrames():(String, ListBuffer[DataFrame]) = {
    var zsaDFs = ListBuffer[DataFrame]();
    var keySpace = "zsa_raw_data";
    var selectCols = Array[String]();

    selectCols = Array("DocumentID", "MainNarrativeSequenceNumber", "MainNarrativeText");
    var df = getTableDF(keySpace, "MainNarrativeInformation", selectCols, true); 
    df = df.withColumn("MainNarrativeSequenceNumber", col("MainNarrativeSequenceNumber").cast(IntegerType));
    df = df.groupBy("DocumentID").agg(zsaNarrativeConcatenator(df("MainNarrativeText"), df("MainNarrativeSequenceNumber")).as("narrative"))
    df = df.withColumn("ID", uuidUDF(col("DocumentID")));
    df = df.withColumnRenamed("DocumentID", "zsa_document_id");
    df = df.select("ID", "zsa_document_id", "narrative");

    zsaDFs.append(df);
    
    return ("zsa_raw_data.mainnarrativetext", zsaDFs);
}

def getZSAMainDataFrames():(String, ListBuffer[DataFrame]) = {
    var zsaDFs = ListBuffer[DataFrame]();
    var keySpace = "zsa_raw_data";
    var selectCols = Array[String]();
    
    //load the parent raw table
    selectCols = Array("ID", "MainID", "ActionTypeCode", "MainName", "MainTypeCodeDescription", "ESavedPriorDocumentNumber", "SavedDate", "SavedEntryDate", "SavedInstitutionNotetoAuth", "SavedReceivedDate", "FormIDDescription", "ValidRegistrationIndicator");
    var zsaRawMainTableDF = getTableDF(keySpace, "Main", selectCols, true); 
    zsaRawMainTableDF = zsaRawMainTableDF.withColumnRenamed("ActionTypeCode", "action_type");
    zsaRawMainTableDF = zsaRawMainTableDF.withColumnRenamed("MainName", "name");
    zsaRawMainTableDF = zsaRawMainTableDF.withColumnRenamed("MainTypeCodeDescription", "type_description")
    zsaRawMainTableDF = zsaRawMainTableDF.withColumnRenamed("ESavedPriorDocumentNumber", "esaved_prior_document_number"); 
    zsaRawMainTableDF = zsaRawMainTableDF.withColumnRenamed("SavedDate", "saved_date");
    zsaRawMainTableDF = zsaRawMainTableDF.withColumnRenamed("SavedEntryDate", "saved_entry_date");
    zsaRawMainTableDF = zsaRawMainTableDF.withColumnRenamed("SavedInstitutionNotetoAuth", "saved_institution_note_to_auth");
    zsaRawMainTableDF = zsaRawMainTableDF.withColumnRenamed("SavedReceivedDate", "saved_received_date");
    zsaRawMainTableDF = zsaRawMainTableDF.withColumnRenamed("FormIDDescription", "form_id_description");
    zsaRawMainTableDF = zsaRawMainTableDF.withColumnRenamed("ValidRegistrationIndicator", "valid_registration");

    //load the main association child table
    selectCols = Array("MainID", "ContinuingMainReportIndicator", "CorrectsAmendsPriorReportIndicator", "AuthDirectBackFileIndicator", "InitialReportIndicator", "JointReportIndicator", "PriorZSAID");
    var zsaRawMainAssociationTableDF = getTableDF(keySpace, "MainAssociation", selectCols, false, true, "MainID"); 
    zsaRawMainAssociationTableDF = zsaRawMainAssociationTableDF.withColumnRenamed("MainID", "MainAssociationMainID");
    zsaRawMainAssociationTableDF = zsaRawMainAssociationTableDF.withColumnRenamed("ContinuingMainReportIndicator", "continuing_main_report");
    zsaRawMainAssociationTableDF = zsaRawMainAssociationTableDF.withColumnRenamed("CorrectsAmendsPriorReportIndicator", "corrects_amends_prior_report");
    zsaRawMainAssociationTableDF = zsaRawMainAssociationTableDF.withColumnRenamed("AuthDirectBackFileIndicator", "auth_direct_back_file");
    zsaRawMainAssociationTableDF = zsaRawMainAssociationTableDF.withColumnRenamed("InitialReportIndicator", "initial_report");
    zsaRawMainAssociationTableDF = zsaRawMainAssociationTableDF.withColumnRenamed("JointReportIndicator", "joint_report");
    zsaRawMainAssociationTableDF = zsaRawMainAssociationTableDF.withColumnRenamed("PriorZSAID", "prior_zsa_id");

    //load the flagged main child table
    selectCols = Array("MainID", "CumulativeTotalViolationAmountText", "FlaggedMainFromDate", "FlaggedMainToDate", "TotalFlaggedAmount");
    var zsaRawSuspciousMainTableDF = getTableDF(keySpace, "FlaggedMain", selectCols, false, true, "MainID"); 
    zsaRawSuspciousMainTableDF = zsaRawSuspciousMainTableDF.withColumnRenamed("MainID", "FlaggedMainMainID");
    zsaRawSuspciousMainTableDF = zsaRawSuspciousMainTableDF.withColumnRenamed("CumulativeTotalViolationAmountText", "cumulative_total_violation_amount");
    zsaRawSuspciousMainTableDF = zsaRawSuspciousMainTableDF.withColumnRenamed("FlaggedMainFromDate", "flagged_main_from_date");
    zsaRawSuspciousMainTableDF = zsaRawSuspciousMainTableDF.withColumnRenamed("FlaggedMainToDate", "flagged_main_to_date");
    zsaRawSuspciousMainTableDF = zsaRawSuspciousMainTableDF.withColumnRenamed("TotalFlaggedAmount", "total_flagged_amount");

    //load the suspcicious main label child table
    selectCols = Array("MainID", "OtherFlaggedMainTypeText", "FlaggedMainTypeCodeDescription", "FlaggedMainSubtypeCodeDescription");
    var zsaRawSuspciousMainLabelTableDF = getTableDF(keySpace, "FlaggedMainLabel", selectCols, false, true, "MainID"); 
    zsaRawSuspciousMainLabelTableDF = zsaRawSuspciousMainLabelTableDF.withColumnRenamed("MainID", "FlaggedMainLabelMainID");
    zsaRawSuspciousMainLabelTableDF = zsaRawSuspciousMainLabelTableDF.withColumnRenamed("OtherFlaggedMainTypeText", "other_flagged_main_type");
    zsaRawSuspciousMainLabelTableDF = zsaRawSuspciousMainLabelTableDF.withColumnRenamed("FlaggedMainTypeCodeDescription", "flagged_main_type");
    zsaRawSuspciousMainLabelTableDF = zsaRawSuspciousMainLabelTableDF.withColumnRenamed("FlaggedMainSubtypeCodeDescription", "flagged_main_subtype");

    //load the elect event indicators child table
    selectCols = Array("MainID", "ElectEventIndicatorsTypeCodeDescription");
    var zsaRawElectEventIndicatorsTableDF = getTableDF(keySpace, "ElectEventIndicators", selectCols, false, true, "MainID"); 
    zsaRawElectEventIndicatorsTableDF = zsaRawElectEventIndicatorsTableDF.withColumnRenamed("MainID", "ElectEventIndicatorsMainID");
    zsaRawElectEventIndicatorsTableDF = zsaRawElectEventIndicatorsTableDF.withColumnRenamed("ElectEventIndicatorsTypeCodeDescription", "elect_event_type");

    //load the remote account main table
    selectCols = Array("MainID", "RemoteAccountHeldQuantity", "ReportCalendarYearText");
    var zsaRawRemoteAccountMainTableDF = getTableDF(keySpace, "RemoteAccountMain", selectCols, false, true, "MainID"); 
    zsaRawRemoteAccountMainTableDF = zsaRawRemoteAccountMainTableDF.withColumnRenamed("MainID", "RemoteAccountMainMainID");
    zsaRawRemoteAccountMainTableDF = zsaRawRemoteAccountMainTableDF.withColumnRenamed("RemoteAccountHeldQuantity", "remote_account_held_quantity");
    zsaRawRemoteAccountMainTableDF = zsaRawRemoteAccountMainTableDF.withColumnRenamed("ReportCalendarYearText", "report_calendar_year");

    //join the tables based on MainID
    var dfSubsets = subsetDataFrame(zsaRawMainTableDF, "ID", largeTableSubsets);
    dfSubsets.foreach((subset) => {
        var joinedDF = subset.join(zsaRawMainAssociationTableDF, col("MainID") === col("MainAssociationMainID"), "left_outer");
        joinedDF = joinedDF.join(zsaRawSuspciousMainTableDF, col("MainID") === col("FlaggedMainMainID"), "left_outer");
        joinedDF = joinedDF.join(zsaRawSuspciousMainLabelTableDF, col("MainID") === col("FlaggedMainLabelMainID"), "left_outer");
        joinedDF = joinedDF.join(zsaRawElectEventIndicatorsTableDF, col("MainID") === col("ElectEventIndicatorsMainID"), "left_outer");
        joinedDF = joinedDF.join(zsaRawRemoteAccountMainTableDF, col("MainID") === col("RemoteAccountMainMainID"), "left_outer");

        joinedDF = joinedDF.select("ID", "action_type", "name", "type_description", "esaved_prior_document_number", "saved_date",  "saved_entry_date", "saved_institution_note_to_auth", "saved_received_date", "form_id_description", "valid_registration", "continuing_main_report", "corrects_amends_prior_report", "auth_direct_back_file", "initial_report", "joint_report", "prior_zsa_id", "cumulative_total_violation_amount", "flagged_main_from_date", "flagged_main_to_date", "total_flagged_amount", "other_flagged_main_type", "flagged_main_type", "flagged_main_subtype", "elect_event_type", "remote_account_held_quantity", "report_calendar_year");

        zsaDFs.append(joinedDF);
    });

    return ("zsa_raw_data.main", zsaDFs);
}

def getZSAEntityDataFrames():(String, ListBuffer[DataFrame]) = {
    var zsaDFs = ListBuffer[DataFrame]();
    var keySpace = "zsa_raw_data";
    var selectCols = Array[String]();

    //load the parent raw table
    selectCols = Array("ID", "EntityID", "MainEntityTypeCodeDescription", "AdmissionConfessionNoIndicator", "AdmissionConfessionYesIndicator", "BothPurchaserSenderSenderReceiveIndicator", "ContactDate", "FemaleGenderIndicator", "MaleGenderIndicator", "UnknownGenderIndicator", "IndividualBirthDate", "LossToFinancialAmount", "OtherCitizenshipCountryTypeDescription", "EntityAsEntityOrganizationIndicator", "EntityTypeCodeDescription", "SenderReceiverIndicator", "PurchaserSenderIndicator");
    var zsaRawEntityTableDF = getTableDF(keySpace, "Entity", selectCols, true); 
    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumnRenamed("MainEntityTypeCodeDescription", "main_type");
    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumnRenamed("AdmissionConfessionNoIndicator", "admission_confession_no");
    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumnRenamed("AdmissionConfessionYesIndicator", "admission_confession_yes");
    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumnRenamed("BothPurchaserSenderSenderReceiveIndicator", "both_purchaser_sender_sender_receiver");
    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumnRenamed("ContactDate", "contact_date");
    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumnRenamed("FemaleGenderIndicator", "female");
    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumnRenamed("IndividualBirthDate", "date_of_birth");
    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumnRenamed("MaleGenderIndicator", "male");
    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumnRenamed("UnknownGenderIndicator", "unknown_gender");
    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumnRenamed("LossToFinancialAmount", "loss_to_financial_amount");
    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumnRenamed("OtherCitizenshipCountryTypeDescription", "citizenship_country");
    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumnRenamed("EntityAsEntityOrganizationIndicator", "as_entity_organization");
    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumnRenamed("EntityTypeCodeDescription", "type");
    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumnRenamed("SenderReceiverIndicator", "sender_receiver");
    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumnRenamed("PurchaserSenderIndicator", "purchaser_sender");

    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumn("is_person", when(col("type") === "Individual", true).otherwise(false));
    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumn("is_organization", when(col("type") === "Organization", true).otherwise(false));
    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumn("is_person_probability", lit(1.0f));
    zsaRawEntityTableDF = zsaRawEntityTableDF.withColumn("is_organization_probability", lit(1.0f));

    //load the entityname child table
    selectCols = Array("EntityID", "AugmentedEntityFullName", "AugmentedIndividualFirstName", "AugmentedIndividualMiddleName", "AugmentedEntityIndividualLastName", "EntityNameTypeCodeDescription");
    var zsaRawEntityNameTableDF = getTableDF(keySpace, "EntityName", selectCols, false, true, "EntityID"); 
    zsaRawEntityNameTableDF = zsaRawEntityNameTableDF.withColumnRenamed("EntityID", "EntityNameEntityID");
    zsaRawEntityNameTableDF = zsaRawEntityNameTableDF.withColumnRenamed("AugmentedEntityFullName", "name");
    zsaRawEntityNameTableDF = zsaRawEntityNameTableDF.withColumnRenamed("AugmentedIndividualFirstName", "first_name");
    zsaRawEntityNameTableDF = zsaRawEntityNameTableDF.withColumnRenamed("AugmentedIndividualMiddleName", "middle_name");
    zsaRawEntityNameTableDF = zsaRawEntityNameTableDF.withColumnRenamed("AugmentedEntityIndividualLastName", "last_name");
    zsaRawEntityNameTableDF = zsaRawEntityNameTableDF.withColumnRenamed("EntityNameTypeCodeDescription", "name_type");

    //load the address child table
    selectCols = Array("EntityID", "AugmentedFullAddressText", "AugmentedCityText", "AugmentedZipCode", "AugmentedStateCodeText", "AugmentedCountryText");
    var zsaRawAddressTableDF = getTableDF(keySpace, "Address", selectCols, false, true, "EntityID"); 
    zsaRawAddressTableDF = zsaRawAddressTableDF.withColumnRenamed("EntityID", "AddressEntityID");
    zsaRawAddressTableDF = zsaRawAddressTableDF.withColumnRenamed("AugmentedFullAddressText", "address");
    zsaRawAddressTableDF = zsaRawAddressTableDF.withColumnRenamed("AugmentedCityText", "city");
    zsaRawAddressTableDF = zsaRawAddressTableDF.withColumnRenamed("AugmentedZipCode", "zip_code");
    zsaRawAddressTableDF = zsaRawAddressTableDF.withColumnRenamed("AugmentedStateCodeText", "state");
    zsaRawAddressTableDF = zsaRawAddressTableDF.withColumnRenamed("AugmentedCountryText", "country");

    //load the onlineaddress child table
    selectCols = Array("EntityID", "OnlineAddressText", "OnlineAddressTypeCodeDescription");
    var zsaRawOnlineAddressTableDF = getTableDF(keySpace, "OnlineAddress", selectCols, false, true, "EntityID"); 
    zsaRawOnlineAddressTableDF = zsaRawOnlineAddressTableDF.withColumnRenamed("EntityID", "OnlineAddressEntityID");
    zsaRawOnlineAddressTableDF = zsaRawOnlineAddressTableDF.withColumnRenamed("OnlineAddressText", "email_address");
    zsaRawOnlineAddressTableDF = zsaRawOnlineAddressTableDF.withColumnRenamed("OnlineAddressTypeCodeDescription", "online_address_type");

    //load the phonenumber child table
    selectCols = Array("EntityID", "PhoneNumberText", "PhoneNumberExtensionText", "PhoneNumberTypeCodeDescription");
    var zsaRawPhoneNumberTableDF = getTableDF(keySpace, "PhoneNumber", selectCols, false, true, "EntityID"); 
    zsaRawPhoneNumberTableDF = zsaRawPhoneNumberTableDF.withColumnRenamed("EntityID", "PhoneNumberEntityID");
    zsaRawPhoneNumberTableDF = zsaRawPhoneNumberTableDF.withColumnRenamed("PhoneNumberText", "phone_number");
    zsaRawPhoneNumberTableDF = zsaRawPhoneNumberTableDF.withColumnRenamed("PhoneNumberExtensionText", "phone_number_extension");
    zsaRawPhoneNumberTableDF = zsaRawPhoneNumberTableDF.withColumnRenamed("PhoneNumberTypeCodeDescription", "phone_number_type");
    
    //load the EntityIdentification child table
    selectCols = Array("EntityID", "OtherIssuerStateDescription", "OtherIssuerCountryDescription", "OtherEntityIdentificationTypeText", "EntityIdentificationNumberText", "EntityIdentificationTypeCodeDescription", "TINUnknownIndicator");
    var zsaRawEntityIdentificationTableDF = getTableDF(keySpace, "EntityIdentification", selectCols, false, true, "EntityID"); 
    zsaRawEntityIdentificationTableDF = zsaRawEntityIdentificationTableDF.withColumnRenamed("EntityID", "EntityIdentificationEntityID");
    zsaRawEntityIdentificationTableDF = zsaRawEntityIdentificationTableDF.withColumnRenamed("OtherIssuerStateDescription", "other_issuer_state");
    zsaRawEntityIdentificationTableDF = zsaRawEntityIdentificationTableDF.withColumnRenamed("OtherIssuerCountryDescription", "other_issuer_country");
    zsaRawEntityIdentificationTableDF = zsaRawEntityIdentificationTableDF.withColumnRenamed("OtherEntityIdentificationTypeText", "other_identification_type");
    zsaRawEntityIdentificationTableDF = zsaRawEntityIdentificationTableDF.withColumnRenamed("EntityIdentificationNumberText", "identification_number");
    zsaRawEntityIdentificationTableDF = zsaRawEntityIdentificationTableDF.withColumnRenamed("EntityIdentificationTypeCodeDescription", "identification_type");
    zsaRawEntityIdentificationTableDF = zsaRawEntityIdentificationTableDF.withColumnRenamed("TINUnknownIndicator", "tin_unknown");

    //join the tables based on EntityID
    var dfSubsets = subsetDataFrame(zsaRawEntityTableDF, "ID", largeTableSubsets);
    dfSubsets.foreach((subs) => {
        var subset = subs;
        var joinedDF = subset.join(zsaRawEntityNameTableDF, col("EntityID") === col("EntityNameEntityID"), "left_outer");
        joinedDF = joinedDF.join(zsaRawAddressTableDF, col("EntityID") === col("AddressEntityID"), "left_outer");
        joinedDF = joinedDF.join(zsaRawOnlineAddressTableDF, col("EntityID") === col("OnlineAddressEntityID"), "left_outer");
        joinedDF = joinedDF.join(zsaRawPhoneNumberTableDF, col("EntityID") === col("PhoneNumberEntityID"), "left_outer");
        joinedDF = joinedDF.join(zsaRawEntityIdentificationTableDF, col("EntityID") === col("EntityIdentificationEntityID"), "left_outer");
        joinedDF = joinedDF.select("ID", "is_person", "is_organization", "is_person_probability", "is_organization_probability", "name", "first_name", "middle_name", "last_name", "name_type", "address", "date_of_birth", "email_address", "online_address_type", "phone_number", "phone_number_extension", "phone_number_type", "main_type", "admission_confession_no", "admission_confession_yes", "contact_date", "female", "male", "unknown_gender", "loss_to_financial_amount", "citizenship_country", "as_entity_organization", "type", "sender_receiver", "purchaser_sender", "both_purchaser_sender_sender_receiver", "other_issuer_state", "other_issuer_country", "other_identification_type", "identification_number", "identification_type", "tin_unknown");
        zsaDFs.append(joinedDF);
    });
    
    return ("zsa_raw_data.entity", zsaDFs);
}

def getSDNEntityDataFrames() = {
    var dfs = ListBuffer[DataFrame]();
    var keySpace = "ofac_raw_data";
    var selectCols = Array[String]();

    selectCols = Array("ID", "uid", "first_name", "last_name", "sdn_type", "title", "remarks");
    var sdnRawEntryTableDF = getTableDF(keySpace, "sdn_entry", selectCols, true); 

    selectCols = Array("uid", "address1", "address2", "address3", "state_or_province", "country", "postal_code", "city");
    var sdnRawAddressTableDF = getTableDF(keySpace, "address", selectCols, false); 
    sdnRawAddressTableDF = sdnRawAddressTableDF.withColumnRenamed("uid", "address_uid");
    sdnRawAddressTableDF = sdnRawAddressTableDF.withColumn("address", concat_ws(",", col("address1"), col("address2"), col("address3")));
    sdnRawAddressTableDF = sdnRawAddressTableDF.withColumnRenamed("state_or_province", "state");
    sdnRawAddressTableDF = sdnRawAddressTableDF.withColumnRenamed("postal_code", "zip_code");
    
    selectCols = Array("uid", "first_name", "last_name", "type", "category");
    var sdnRawAkaTableDF = getTableDF(keySpace, "aka", selectCols, false); 
    sdnRawAkaTableDF = sdnRawAkaTableDF.withColumnRenamed("uid", "aka_uid");
    sdnRawAkaTableDF = sdnRawAkaTableDF.withColumnRenamed("first_name", "aka_first_name");
    sdnRawAkaTableDF = sdnRawAkaTableDF.withColumnRenamed("last_name", "aka_last_name");

    selectCols = Array("parent_id", "program");
    var sdnRawProgramTableDF = getTableDF(keySpace, "program_list", selectCols, false); 
    sdnRawProgramTableDF = sdnRawProgramTableDF.withColumnRenamed("program", "sdn_program");

    sdnRawEntryTableDF = sdnRawEntryTableDF.withColumn("is_person", when(col("sdn_type") === "Individual", true).otherwise(false));
    sdnRawEntryTableDF = sdnRawEntryTableDF.withColumn("is_organization", when(col("sdn_type") === "Entity", true).otherwise(false));
    sdnRawEntryTableDF = sdnRawEntryTableDF.withColumn("is_person_probability", lit(1.0f));
    sdnRawEntryTableDF = sdnRawEntryTableDF.withColumn("is_organization_probability", lit(1.0f));
    sdnRawEntryTableDF = sdnRawEntryTableDF.withColumn("name", when(col("sdn_type") === "Entity", col("last_name")).otherwise(concat(col("first_name"), lit(" "), col("last_name"))));

    var joinedDF = sdnRawEntryTableDF.join(sdnRawAkaTableDF, col("uid") === col("aka_uid"), "left_outer");
    joinedDF = joinedDF.join(sdnRawAddressTableDF, col("uid") === col("address_uid"), "left_outer");
    joinedDF = joinedDF.join(sdnRawProgramTableDF, col("ID") === col("parent_id"), "left_outer");
    joinedDF = joinedDF.select("ID", "is_person", "is_organization", "is_person_probability", "is_organization_probability", "first_name", "last_name", "sdn_type", "title", "remarks", "aka_first_name", "aka_last_name", "category", "address", "city", "state", "zip_code", "sdn_program");

    dfs.append(joinedDF);
    
    ("ofac_raw_data.sdn_entry", dfs);
}

def getModelIndividualDataFrames() = {
    var dfs = ListBuffer[DataFrame]();

    var df = getTableDF(dataModelKeySpace, "entity");
    df = df.filter(col("is_person") === lit(true));

    dfs.append(df);

    (s"$dataModelKeySpace.entity", dfs);
}

def getModelOrganizationDataFrames() = {
    var dfs = ListBuffer[DataFrame]();

    var df = getTableDF(dataModelKeySpace, "entity");
    df = df.filter(col("is_organization") === lit(true));

    dfs.append(df);

    (s"$dataModelKeySpace.entity", dfs);
}

def changeColumnTypes(df:DataFrame) = {
    var tableDF = df;

    //convert decimal to float
    var decimalColumns = tableDF.schema.toList.filter(x => x.dataType.isInstanceOf[DecimalType]).map(c => c.name);
    decimalColumns.foreach((colName) => {
        tableDF = tableDF.withColumn(colName, col(colName).cast(FloatType));
    });

    //split out timestamps
    var timestampColumns = tableDF.schema.toList.filter(x => x.dataType.isInstanceOf[TimestampType]).map(c => c.name);
    timestampColumns.foreach((colName) => {
        tableDF = tableDF.withColumn(colName + "_date", col(colName).cast(DateType));
    });

    //all strings to upper
    var stringColumns = tableDF.schema.toList.filter(x => x.dataType.isInstanceOf[StringType]).map(c => c.name);
    var exclude = Array("ID", "id", "in_ID", "out_ID", "root_address", "raw_source_key_space", "raw_source_parent_table");
    stringColumns.foreach((colName) => {
        if (exclude.contains(colName)) {} else {
            tableDF = tableDF.withColumn(colName, upper(col(colName)));
        }
    });

    tableDF;
}

def addSourceInfo(dataFrame:DataFrame, sourceParentTable:String) = {
    var df = dataFrame;
    var ks = sourceParentTable.split("\\.")(0);
    var table = sourceParentTable.split("\\.")(1);
    df = df.withColumn("raw_source_key_space", lit(ks));
    df = df.withColumn("raw_source_parent_table", lit(table));
    df;
}

def meetEntityColumnRequirements(entity:String, df:DataFrame, sourceParentTable:String):DataFrame = {
    var newDF = df;
    newDF = changeColumnTypes(newDF);
    newDF = addSourceInfo(newDF, sourceParentTable)
    return newDF;
}

def meetRelationshipColumnRequirements(relationship:RelDef, df:DataFrame, sourceParentTable:String):DataFrame = {
    var newDF = df;
    newDF = changeColumnTypes(newDF);
    newDF = addSourceInfo(newDF, sourceParentTable);
    return newDF;
}

def saveModelDataFrame(tableName:String, dataFrame:DataFrame) = {
    if (enableCassandra) {
        var df = dataFrame;
        pln(s"""Saving model data frame to table '$tableName'...""");
        saveDataFrame(df, dataModelKeySpace, tableName);
        if (isDebug) {} else {
            waitMilli(120 * 1000);
        }
        pln(s"""Done saving model data frame to table '$tableName'.""");
    }
}

def getDataSetEntityExtractors() = {
    var list = HashMap[String, HashMap[String, () => (String, ListBuffer[DataFrame])]]();
    
    list += ("ZSA" -> HashMap[String, () => (String, ListBuffer[DataFrame])]());
    list += ("CA" -> HashMap[String, () => (String, ListBuffer[DataFrame])]());
    list += ("GT" -> HashMap[String, () => (String, ListBuffer[DataFrame])]());
    list += ("SDN" -> HashMap[String, () => (String, ListBuffer[DataFrame])]());
    list += ("Model" -> HashMap[String, () => (String, ListBuffer[DataFrame])]());
    
    list("ZSA") += ("entity" -> getZSAEntityDataFrames _);
    list("ZSA") += ("main" -> getZSAMainDataFrames _);
    list("ZSA") += ("account" -> getZSAAccountDataFrames _);
    list("ZSA") += ("object" -> getZSAObjectDataFrames _);
    list("ZSA") += ("narrative" -> getZSANarrativeDataFrames _);
    list("ZSA") += ("ip_address" -> getZSAIPAddressDataFrames _)

    list("CA") += ("entity" -> getCAEntityDataFrames _);
    list("CA") += ("cryptocurrency_address" -> getCACryptoCurrencyAddressDataFrames _);
    list("CA") += ("account" -> getCAAccountDataFrames _);
    list("CA") += ("internet_source" -> getCAInternetSourceDataFrames _);
    list("CA") += ("bitcoin_node" -> getCABitcoinNodeDataFrames _);
    list("CA") += ("cryptocurrency_wallet_software" -> getCACryptoCurrencyWalletSoftwareDataFrames _);
    list("CA") += ("location" -> getCALocationDataFrames _);
    list("CA") += ("city" -> getCACityDataFrames _);
    list("CA") += ("country" -> getCACountryDataFrames _);
    list("CA") += ("ip_address" -> getCAIPAddressDataFrames _);

    list("GT") += ("entity" -> getGTEntityDataFrames _);
    list("GT") += ("location" -> getGTLocationDataFrames _);
    list("GT") += ("container" -> getGTContainerDataFrames _);
    list("GT") += ("vessel" -> getGTVesselDataFrames _);
    list("GT") += ("ip_address" -> getGTIPAddresses _);

    list("SDN") += ("entity" -> getSDNEntityDataFrames _);

    list("Model") += ("individual" -> getModelIndividualDataFrames _);
    list("Model") += ("organization" -> getModelOrganizationDataFrames _);

    list;
}

def getDataSetRelationshipExtractors() = {
    var list = HashMap[String, HashMap[RelDef, () => (String, ListBuffer[DataFrame])]]();
    
    list += ("ZSA" -> HashMap[RelDef, () => (String, ListBuffer[DataFrame])]());
    list += ("CA" -> HashMap[RelDef, () => (String, ListBuffer[DataFrame])]());
    list += ("GT" -> HashMap[RelDef, () => (String, ListBuffer[DataFrame])]());
    list += ("SDN" -> HashMap[RelDef, () => (String, ListBuffer[DataFrame])]());
    list += ("Model" -> HashMap[RelDef, () => (String, ListBuffer[DataFrame])]());
    
    var zsaEdges = getZSAEdgeDataFrames();
    
    for ((edgeTableName, dfFunc) <- zsaEdges) {
        list("ZSA") += (edgeTableName -> dfFunc);
    }

    var caEdges = getCAEdgeDataFrames();
    
    for ((edgeTableName, dfFunc) <- caEdges) {
        list("CA") += (edgeTableName -> dfFunc);
    }

    var gtEdges = getGTEdgeDataFrames();
    
    for ((edgeTableName, dfFunc) <- gtEdges) {
        list("GT") += (edgeTableName -> dfFunc);
    }

    var sdnEdges = getSDNEdgeDataFrames();
    
    for ((edgeTableName, dfFunc) <- sdnEdges) {
        list("SDN") += (edgeTableName -> dfFunc);
    }

    var modelEdges = getModelEdgeDataFrames();
    
    for ((edgeTableName, dfFunc) <- modelEdges) {
        list("Model") += (edgeTableName -> dfFunc);
    }

    list;
}

def getIndexDefinitions() = {
    var list = ListBuffer[IndexDef]();
    list.append(IndexDef("cryptocurrency_address.address", "schema.vertexLabel('cryptocurrency_address').searchIndex().ifNotExists().by('address').waitForIndex().create()"));
    list.append(IndexDef("entity.name", "schema.vertexLabel('entity').searchIndex().ifNotExists().by('name').waitForIndex().create()"));
    list.append(IndexDef("entity.address", "schema.vertexLabel('entity').searchIndex().ifNotExists().by('address').waitForIndex().create()"));
    list.append(IndexDef("location.type", "schema.vertexLabel('location').materializedView('location_by_location_type').ifNotExists().partitionBy('location_type').clusterBy('ID', Asc).waitForIndex().create()"));
    list.append(IndexDef("entity.lastname", "schema.vertexLabel('entity').searchIndex().ifNotExists().by('last_name').waitForIndex().create()"));
    list.append(IndexDef("entity.akalastname", "schema.vertexLabel('entity').searchIndex().ifNotExists().by('aka_last_name').create()"));
    list.append(IndexDef("entity.blockchain_root_address", "schema.vertexLabel('entity').secondaryIndex('by_blockchain_root_address').ifNotExists().by('blockchain_root_address').create();"));
    list.append(IndexDef("account.root_address", "schema.vertexLabel('account').secondaryIndex('by_root_address').ifNotExists().by('root_address').create();"));
    list.append(IndexDef("cryptocurrency_address.root_address", "schema.vertexLabel('cryptocurrency_address').secondaryIndex('by_root_address').ifNotExists().by('root_address').create();"));
    list.append(IndexDef("account.number", "schema.vertexLabel('account').secondaryIndex('by_number').ifNotExists().by('number').create();"));
    list;
}

def ensureAllLoad() = {
    for ((dataSet, extractors) <- dataSetEntityExtractors) {
        if (dataSetsToLoad.contains(dataSet)) {
        } else {
            dataSetsToLoad.append(dataSet);
        }
        for ((entity, extractor) <- extractors) {
            if (entitiesToLoad.contains(entity)) {
            } else {
                entitiesToLoad.append(entity);
            }
        }
    }

    for ((dataSet, extractors) <- dataSetRelationshipExtractors) {
        if (dataSetsToLoad.contains(dataSet)) {
        } else {
            dataSetsToLoad.append(dataSet);
        }
        for ((relationship, extractor) <- extractors) {
            if (relationshipsToLoad.contains(relationship)) {
            } else {
                relationshipsToLoad.append(relationship);
            }
        }
    }

    for ((index) <- indexDefinitions) {
        if (indexesToLoad.contains(index)) {
        } else {
            indexesToLoad.append(index);
        }
    }
}

def run() = {
    clearAllCached();

    if (enableTracking) {
        loadTracking(dataModelKeySpace);
        if (trackingReset) {
            resetTracking(dataModelKeySpace);
        }
    }

    if (enableEnsureAllLoad) {
        ensureAllLoad();
    }

    ensureKeySpace();

    dataSetsToLoad.foreach((dataSet) => {
        //nodes
        if (dataSetEntityExtractors.contains(dataSet)) {
            var entityExtractors = dataSetEntityExtractors(dataSet);
            entitiesToLoad.foreach((entity) => {
                if (entityExtractors.contains(entity)) {
                    var counter = 0;
                    var totalCount = 0L;
                    var trackingKey = s"$dataModelKeySpace.$dataSet.$entity";
                    pln("Saving all " + dataSet + " " + entity + "...");
                    if (trackingShouldLoad(dataModelKeySpace, dataSet, entity)) {
                        val elasticIndexName = dataModelKeySpace + "_" + entity;
                        if (elasticDeleteIndexes) {
                            deleteElasticIndex(elasticIndexName);
                        }
                        if (elasticOnlyLoadFromTables) {
                            var df = getTableDF(dataModelKeySpace, entity);
                            df = cacheDF(df);
                            totalCount = df.count();
                            saveDFToElastic(df, elasticIndexName, totalCount);
                        } else {
                            val (sourceParentTable, dfs) = entityExtractors(entity)();
                            dfs.foreach((dataFrame) => {
                                var df = dataFrame;
                                pln("Saving " + dataSet + " " + entity + " data frame " + (counter + 1).toString() + "/" + dfs.size.toString() + "...");
                                df = applyDebugLimit(df);
                                df = meetEntityColumnRequirements(entity, df, sourceParentTable);
                                ensureEntityTableForDataFrame(df, entity); 
                                df = cacheDF(df);
                                val count = df.count();
                                totalCount += count;
                                saveModelDataFrame(getEntityTableName(entity), df);
                                saveDFToElastic(df, elasticIndexName, totalCount);
                                pln("Done saving " + dataSet + " " + entity + " data frame " + (counter + 1).toString() + "/" + dfs.size.toString() + ". Records saved: " + count.toString() + ".");
                                df = df.unpersist();
                                counter += 1;
                            });
                        }
                        clearAllCached();
                        updateTracking(dataModelKeySpace, dataSet, entity, "Entity", totalCount);
                    } else {
                        pln(s"Skipping because tracking is turned on and this has already loaded. Tracking key: $trackingKey.");
                    }
                    pln(s"Done saving all $dataSet $entity. Total saved: $totalCount.");
                }
            });
        }

        //edges
        if (dataSetRelationshipExtractors.contains(dataSet)) {
            var relationshipExtractors = dataSetRelationshipExtractors(dataSet);
            relationshipsToLoad.foreach((relationship) => {
                if (relationshipExtractors.contains(relationship)) {
                    var tableName = getRelationshipTableName(relationship);
                    var counter = 0;
                    var totalCount = 0L;
                    var trackingKey = s"$dataModelKeySpace.$dataSet.$tableName";
                    pln("Saving all " + dataSet + " " + relationship + "...");
                    if (trackingShouldLoad(dataModelKeySpace, dataSet, tableName)) {
                        val (sourceParentTable, dfs) = relationshipExtractors(relationship)();
                        dfs.foreach((dataFrame) => {
                            var df = dataFrame;
                            pln("Saving " + dataSet + " " + relationship + " data frame " + (counter + 1).toString() + "/" + dfs.size.toString() + "...");
                            df = applyDebugLimit(df);
                            df = meetRelationshipColumnRequirements(relationship, df, sourceParentTable);
                            ensureRelationshipTableForDataFrame(df, relationship);
                            df = cacheDF(df);
                            val count = df.count();
                            totalCount += count;
                            saveModelDataFrame(getRelationshipTableName(relationship), df);
                            pln("Done saving " + dataSet + " " + relationship + " data frame " + (counter + 1).toString() + "/" + dfs.size.toString() + ". Records saved: " + count.toString() + ".");
                            df = df.unpersist();
                            counter += 1;
                        });
                        clearAllCached();
                        updateTracking(dataModelKeySpace, dataSet, tableName, "Relationship", totalCount);
                    } else {
                        pln(s"Skipping because tracking is turned on and this has already loaded. Tracking key: $trackingKey.");
                    }
                    pln(s"Done saving all $dataSet $relationship. Total saved: $totalCount.");
                }
            });
        }
    });

    pln("Saving final tracking info...");
    saveTrackingInfoToCSV(dataModelKeySpace, true);
    pln("Done saving final tracking info.");

    if (enableCreateDSEIndexes) {
        pln("Creating indexes...");
        indexesToLoad.foreach((index) => {
            var statement = index.statement;
            if (enableDSEWaitForIndex) {} else {
                statement = statement.replace(".waitForIndex()", "");
            }
            executeGraph(statement);
        });
        pln("Done creating indexes.");
    } 
}

val processStart = LocalDateTime.now();
val s3URIPrefix = "s3a://";
val dataModelKeySpace = "data_model";
val elasticEndpoint = "vpc.us-west-1.es.amazonaws.com";
val isDebug = false;
val debugRecordsToLoad = 100000;
val largeTableSubsets = 20;
val dataSetEntityExtractors = getDataSetEntityExtractors();
val dataSetRelationshipExtractors = getDataSetRelationshipExtractors();
val indexDefinitions = getIndexDefinitions();

var enableEnsureAllLoad = true;
var enableTracking = true;
var enableElastic = true;
var enableCassandra = true;
var enableCreateDSEIndexes = true;
var enableDSEWaitForIndex = false;

var elasticDeleteIndexes = false;
var elasticOnlyLoadFromTables = false;

var trackingSkipLoaded = true;
var trackingReset = false;
var trackingKeySpace = "etl_tracking";

var dataSetsToLoad = ListBuffer[String]();
var entitiesToLoad = ListBuffer[String]();
var relationshipsToLoad = ListBuffer[RelDef]();
var indexesToLoad = ListBuffer[IndexDef]();

dataSetsToLoad = ListBuffer[String]("CA", "ZSA", "GT", "SDN", "Model");
entitiesToLoad = ListBuffer[String]("narrative", "country", "city", "cryptocurrency_wallet_software", "bitcoin_node", "internet_source", "cryptocurrency_address", "object", "account", "main", "vessel", "container", "location", "entity");
relationshipsToLoad = ListBuffer[RelDef](RelDef("has", "entity", "narrative"));
indexesToLoad = ListBuffer[IndexDef]();

run();

val processEnd = LocalDateTime.now();
val duration = Duration.between(processStart, processEnd);

pln("---------------------------------------------------");
pln("Process start:    " + processStart.toString());
pln("Process end:      " + processEnd.toString());
pln("Process run time: " + formatDuration(duration));
