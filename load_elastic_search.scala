/*
Creates an index in Elastic and loads it with data pulled from a Cassandra table.
*/

import spark.implicits._;
import scala.collection.JavaConverters._;
import scala.collection.mutable.{ListBuffer, ListMap, HashMap};
import scala.io.Codec
import scala.io.Source;
import org.apache.spark.rdd.RDD;
import org.apache.commons.io.IOUtils;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.sql.{DataFrame, Row};
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions._;
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder, AmazonS3URI, AmazonS3Client};
import com.amazonaws.services.s3.model._;
import com.amazonaws.auth.{EnvironmentVariableCredentialsProvider, BasicAWSCredentials, AWSStaticCredentialsProvider};
import com.amazonaws.{ClientConfiguration, Protocol};
import com.amazonaws.regions.{Region, Regions};
import java.time.{Duration, LocalDateTime, LocalDate, LocalTime};
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder};
import java.time.temporal.ChronoField;
import java.io.{InputStream, ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream, BufferedReader, InputStreamReader};
import java.util.UUID;
import java.nio.charset._;
import java.lang.ProcessBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods._;
import org.apache.http.entity._;
import org.apache.http.impl.client.HttpClientBuilder;
import org.elasticsearch.spark.sql._;

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

def pln(line: Any) = {
    val dt = LocalDateTime.now();
    println("(EL-" + dt.toString() + "): " + line.toString());
}


def formatDuration(diff: Duration): String = {
    val s = duration.getSeconds() + 1
    return "%d:%02d:%02d".format(s / 3600, (s % 3600) / 60, (s % 60))    
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

def getStrFromInputStream(is: InputStream):String = {
    val codec = Codec("UTF-8");
    codec.onMalformedInput(CodingErrorAction.IGNORE);
    codec.onUnmappableCharacter(CodingErrorAction.IGNORE);
    val pulledString = scala.io.Source.fromInputStream(is)(codec).mkString;
    return pulledString;
}

def getS3Str(s3Client: AmazonS3, bucket: String, key: String, encoding: String = "UTF-8", fix: Boolean = false): String = {
    val objRequest = new GetObjectRequest(bucket, key);
    var stream: InputStream = s3Client.getObject(objRequest).getObjectContent
    if (fix) {
        val bytes = IOUtils.toByteArray(stream)
        stream.close()
        var toBytes = Array.fill[Byte](bytes.length)(0)
        var index = 0
        var toIndex = 0
        while (index < bytes.length && toIndex < toBytes.length) {
            if (bytes(index) == 0) {
                
            } else {
                toBytes(toIndex) = bytes(index)
                toIndex += 1
            }
            index += 1
        }
        stream = new ByteArrayInputStream(toBytes, 0, toIndex + 1)
    }
    val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.IGNORE)
    codec.onUnmappableCharacter(CodingErrorAction.IGNORE)    
    val pulledString = scala.io.Source.fromInputStream(stream)(codec).mkString
    stream.close()
    return pulledString
}

def getIndexCount(indexName:String):Long = {
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

def sendS3FileToES(s3Client: AmazonS3, bucket: String, key: String):HttpResponse = {
    val splitkey = key.replace(".json", "").split("/");
    val id = splitkey(splitkey.length - 1);
    val getRequest = new GetObjectRequest(bucket, key);
    val is: InputStream = s3Client.getObject(getRequest).getObjectContent;
    val entity = new InputStreamEntity(is);
    val req = new HttpPut("http://" + elasticEndpoint + "/" + id);
    req.setEntity(entity);
    req.addHeader("Content-Type", "application/json");
    req.addHeader("Accept","text/plain");
    req.addHeader("Connection","close");
    val clientBuilder = HttpClientBuilder.create();
    val client = clientBuilder.build();
    val response = client.execute(req);
    return response;
}

def handleBatch(batch:ListBuffer[String]) = {
    var batchRDD = sc.parallelize(batch);
    batchRDD.foreach((path) => {
        var s3Client = getS3Client;
        var response = sendS3FileToES(s3Client, s3Bucket, path);
        var statusCode = response.getStatusLine().getStatusCode();
        if (statusCode > 299 || statusCode < 200) {
            if (isDebug) {
                println(response);
                println(getStrFromInputStream(response.getEntity.getContent));
            }
        }
    })
}

def loadFullDocs(s3Bucket: String, s3Location: String): Unit = {
    val s3Client = getS3Client;
    val dirIterator = new S3DirIterator(s3Bucket, s3Location, s3Client, Array(".json"));
    var listing = dirIterator.getNextListing();
    var counter = 0;
    val batchSize = 10000;
    var batch = ListBuffer[String]();
    var totalLoaded = 0;
    
    while (listing != Nil && listing.size > 0) {
        listing.foreach((path) => {
            batch.append(path); 
        });

        if (batch.size >= batchSize) {
            handleBatch(batch);
            totalLoaded += batch.size;
            if (totalLoaded % 1000000 == 0) {
                pln("Total loaded so far: " + totalLoaded.toString + ". Batch size: " + batchSize.toString + ".");
            }
            batch = ListBuffer[String]();    
        }

        listing = dirIterator.getNextListing(); 

        if (isDebug) {
            counter += 1;
            if (counter > 20) {
                listing = Nil;
            }
        }
    }
    
    if (batch.size > 0) {
        handleBatch(batch);
        totalLoaded += batch.size;
        batch = ListBuffer[String]();    
    }
    
    pln("Done. Total loaded: " + totalLoaded.toString + ".");
}

var elasticEndpoint = "vpc-muxcglvjq.us-west-1.es.amazonaws.com";
val s3Bucket = "data";
val s3Location = "JSON/";

def run() = {
    pln("Loading keyspace tables...");
    
    var keySpaceColumnName = "databaseName";
    var excludeKeySpaces = Array("");
    
    var keySpacesDF = spark.sql("SHOW SCHEMAS");
    keySpacesDF = keySpacesDF.select(keySpaceColumnName);
    keySpacesDF = keySpacesDF.filter(!col(keySpaceColumnName).isin(excludeKeySpaces:_*));
    
    var keySpaces = keySpacesDF.collect().map(row => row.getString(0));
    
    var keySpaceIndex = 0;
    while (keySpaceIndex < keySpaces.length) {
        var keySpace = keySpaces(keySpaceIndex);
        spark.sql("USE " + keySpace);
        var tablesDF = spark.sql("SHOW TABLES");
        tablesDF = tablesDF.select("tableName");
        tablesDF = tablesDF.filter($"isTemporary" === false);
    
        var tables = tablesDF.collect().map(row => row.getString(0));
        
        var tableIndex = 0;
        while (tableIndex < tables.length) {
            var table = tables(tableIndex);
            
            //check to make sure we have an ID field. If not, skip. Will need to implement a way to get the complete partition key and use that as the ID column. If the partition key is comprised of multiple columns, will need to concatenate those values into a single DF column to be used by ES
            var idColumnName = "ID";
            var tableFieldsDF = spark.sql("SHOW COLUMNS IN " + table);
            tableFieldsDF = tableFieldsDF.filter($"col_name" === idColumnName);
            var idColumnCount = tableFieldsDF.count();
            
            if (idColumnCount > 0) {
                pln("Working on " + keySpace + "." + table + "...");

                var indexName = (keySpace + "_" + table).toLowerCase();
                var indexCount = getIndexCount(indexName);  
                pln("Elastic index count is " + indexCount.toString() + ".");
                
                pln("Loading " + keySpace + "." + table + "...");
                var dataDF = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace", keySpace).option("table", table).load();

                //need to cast decimal types to something else because ES can't handle decimal types (wtf)
                var decimalColumns = dataDF.schema.toList.filter(x => x.dataType.isInstanceOf[DecimalType]).map(c => c.name);
                decimalColumns.foreach((colName) => {
                    dataDF = dataDF.withColumn(colName, col(colName).cast(FloatType));
                })
                
                //treat empty strings as null for ES
                var stringColumns = dataDF.schema.toList.filter(x => x.dataType.isInstanceOf[StringType]).map(c => c.name);
                stringColumns.foreach((colName) => {
                    dataDF = dataDF.withColumn(colName, when(col(colName) === "", null).otherwise(col(colName)));
                })

                //convert timestamps to strings because ES treats them as a number and does its own type inferencing anyway.
                var dateColumns = dataDF.schema.toList.filter(x => x.dataType.isInstanceOf[TimestampType]).map(c => c.name);
                dateColumns.foreach((colName) => {
                    dataDF = dataDF.withColumn(colName, regexp_replace(col(colName).cast(StringType), lit(" "), lit("T")));
                })
                
                if (isDebug) {
                    dataDF = dataDF.limit(1000);
                }
                dataDF.persist(StorageLevel.MEMORY_AND_DISK);
                var dfCount = dataDF.count();
                pln ("Table count is " + dfCount.toString() + ".");
                pln("Done loading " + keySpace + "." + table + ".");
                
                if (dfCount > indexCount) {
                    pln("Saving " + keySpace + "." + table + " to ElasticSearch index '" + indexName + "'...");
                    var esConf = Map("es.nodes" -> elasticEndpoint, "es.port" -> "80", "es.write.operation" -> "upsert", "es.mapping.id" -> idColumnName, "es.nodes.wan.only" -> "true", "es.field.read.empty.as.null" -> "true");
                    dataDF.saveToEs(indexName, esConf);
                    pln("Done saving " + keySpace + "." + table + " to ElasticSearch '" + indexName + "' index.");
                } else {
                    pln("Skipping " + indexName + " because it is already loaded.");
                }
                pln("Done working on " + keySpace + "." + table + ".");
            } else {
                pln("Skipping table " + keySpace + "." + table + " because it does not contain an ID column. Will need to add logic to enumerate the partition key columns for each table and use it to map to the docID in ES so that upserts can be accomplished.");
            }
    
            tableIndex += 1;    
        }
        
        keySpaceIndex += 1;
    }
    
    pln("Done loading keyspace tables.");
    
    pln("Loading complete docs...");
    loadFullDocs(s3Bucket, s3Location);
    pln("Done loading complete docs.");
}

val processStart = LocalDateTime.now();
val isDebug = false;

run();

val processEnd = LocalDateTime.now();
val duration = Duration.between(processStart, processEnd);

pln("---------------------------------------------------");
pln("Process start:    " + processStart.toString());
pln("Process end:      " + processEnd.toString());
pln("Process run time: " + formatDuration(duration));
