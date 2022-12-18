/*
When Spark loads a large table into a DataFrame, it is partitioned across the spark cluster. 
When the dataframe is persisted to S3 (or HDFS, other file systems, etc), each partition is written to the file system concurrently.
Spark's DataFrame does not (at the time this was written) support writing to a single file when the data is too large to fit on a single node in the spark cluster.
Saves a Cassandra table as a single CSV regardless of table size. 
This is accomplished by:
1. Loading the table into a DataFrame
2. Saving the DataFrame to S3, allowing the file to be partitioned
3. Iterating over the saved partitions and concurrently streaming them as a multi-part single S3 object
*/
import spark.implicits._;
import scala.collection.JavaConverters._;
import scala.collection.mutable.{ListBuffer, ListMap, HashMap};
import org.apache.spark.rdd.RDD;
import org.apache.commons.io.IOUtils;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.sql.{DataFrame, Row};
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder, AmazonS3URI, AmazonS3Client};
import com.amazonaws.services.s3.model._;
import com.amazonaws.auth.{EnvironmentVariableCredentialsProvider, BasicAWSCredentials, AWSStaticCredentialsProvider};
import com.amazonaws.{ClientConfiguration, Protocol};
import com.amazonaws.regions.{Region, Regions};
import java.time.{Duration, LocalDateTime, LocalDate, LocalTime};
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder};
import java.time.temporal.ChronoField;
import java.io.{InputStream, ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream, BufferedReader, InputStreamReader};

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

def getS3KeyFileName(key: String): String = {
    if (key.contains("/")) {
        return key.substring(key.lastIndexOf("/") + 1)
    }
    return ""
}

def pln(line: Any) = {
    val dt = LocalDateTime.now();
    println("(" + dt.toString() + "): " + line.toString());
}

def formatDuration(diff: Duration): String = {
    val s = duration.getSeconds() + 1
    return "%d:%02d:%02d".format(s / 3600, (s % 3600) / 60, (s % 60))    
}

def ensureEOL(str: String): String = {
    if (str.endsWith("\n")) {
        
    } else {
        return str + "\n";
    }
    return str;
}

def getS3Client(uname:String = "", pwd:String = ""): AmazonS3 = {
    var creds = new BasicAWSCredentials(uname, pwd);
    var clientConfig = new ClientConfiguration();
    clientConfig.setProtocol(Protocol.valueOf("HTTPS"));
    var client = new AmazonS3Client(creds, clientConfig);
    val endpoint = "s3-website.us-west-1.amazonaws.com";
    client.setEndpoint(endpoint);
    client.setRegion(Region.getRegion(Regions.Cloud));
    return client;
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
    val metadata = new ObjectMetadata();
    metadata.setContentType(contentType);
    metadata.setContentLength(bytes.length);
    val request = new PutObjectRequest(bucket, key, inputStream, metadata);
    s3Client.putObject(request);
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

def run() = {
    val keySpace = "raw_data";
    val tableName = "Activity";
    val s3Client = getS3Client;
    val destBucket = "timt-dev";
    val destKey = "singleCSVExample.csv";
    
    pln("Getting dataframe from Cassandra...");
    
    var df = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace", keySpace).option("table", tableName).load();

    if (isDebug) {
        df = df.limit(debugRecordsPerFile);
    }
    
    pln("Done getting dataframe from Cassandra.");

    pln("Saving as single CSV...");
    
    ///////
    saveDataFrameAsOneCSV(s3Client, df, destBucket, destKey); ///////
    ///////
    
    pln("Done saving as CSV.");
}

val processStart = LocalDateTime.now();
val isDebug = false;
val debugRecordsPerFile = 50000;
val s3URIPrefix = "s3a://";

run();

val processEnd = LocalDateTime.now();
val duration = Duration.between(processStart, processEnd);

pln("---------------------------------------------------");
pln("Process start:    " + processStart.toString());
pln("Process end:      " + processEnd.toString());
pln("Process run time: " + formatDuration(duration));
