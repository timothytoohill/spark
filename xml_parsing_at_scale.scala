/*
This iterates an S3 dir for XML files and paralellizes their parsing. It opens indidivual read streams and distributes the parsing across the cluster.
Each level of the XML hierarchy is assumed to be a new entity, and any child nodes are assumed to have a "has" relationship with its parent.
The XML hierarchy is recursively processed and tabularized. The XML is converted and saved as both JSON and into tables tha represent each entity.
Child, Parent, and SourceDoc columns are added to the CSV files to represent the hierarchy in the XML file.
*/
import scala.collection.JavaConverters._
import scala.xml._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListMap
import scala.util.parsing.json._
import scala.util.Sorting
import scala.io.Codec
import java.io.InputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import java.io.InputStreamReader
import java.io.File
import java.io.FileWriter
import java.io.BufferedReader
import java.nio.charset._
import java.time.LocalDateTime
import java.time.LocalDate
import java.time.LocalTime
import java.time.temporal.TemporalAccessor
import java.time.Duration
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.security.MessageDigest
import java.lang.ProcessBuilder
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.model._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.commons.io.FileUtils
import org.apache.spark.util.MutableURLClassLoader
import org.apache.commons.io.IOUtils

type ZSATable = ListBuffer[HashMap[String, String]]
type ZSATables = HashMap[String, ZSATable]
type ZSASchemaTableField = HashMap[String, HashMap[String, Long]]
type ZSASchemaTable = HashMap[String, ZSASchemaTableField]
type ZSAHashMap = HashMap[String, Any]
type ErrorRow = HashMap[String, String]
type ErrorTable = ListBuffer[HashMap[String, String]]

class ProcessResult(var documentCount: Long = 0, var zsaSchemaTable: ZSASchemaTable = HashMap[String, ZSASchemaTableField](), var errors: ErrorTable = ListBuffer[ErrorRow](), var loadScript: String = "") extends Serializable {
    override def toString(): String = {
        var map = HashMap[String, Any]()
        var results = HashMap[String, Any]()
        map += ("ProcessResult" -> results)
        
        results += ("DocumentCount" -> documentCount)
        results += ("ErrorCount" -> errors.length)
        results += ("Errors" -> errors)
        results += ("ZSASchemaTableCount" -> zsaSchemaTable.size)
        results += ("LoadScript" -> loadScript)
        
        return toJSON(map).replace("\n", " ").replace("\t", "")
    }
}

class ZSAXMLMainDocumentStringIterator(bucket: String, key: String, bufferSize: Int = 10000000, s3Client: AmazonS3 = getS3Client) extends Serializable {
    private var lastFileIndex: Long = 0
    private var lastStrIndex = 0
    private var doneReading = false
    private val beginTag1 = "<Main "
    private val beginTag2 = "<Main>"
    private val endTag = "</Main>"
    private var objectSize: Long = 0
    private var buffSize = bufferSize
    private var currentString = new StringBuilder(buffSize)    
    private var initialized = false

    
    def initialize() = {
        if (!initialized) {
            objectSize = s3Client.getObjectMetadata(bucket, key).getContentLength
            buffSize = bufferSize
            initialized = true
            
            if (objectSize < buffSize) {
                buffSize = objectSize.toInt
            }
        
            currentString = new StringBuilder(buffSize)    
        }
    }
    
    def getNextDocument(): String = {
        if (!doneReading) {
            initialize()
            
            if (currentString.length >= buffSize) {
            } else {
                val getRequest = new GetObjectRequest(bucket, key).withRange(lastFileIndex, lastFileIndex + buffSize - 1)
                val is: InputStream = s3Client.getObject(getRequest).getObjectContent
                val bytes = IOUtils.toByteArray(is);
                is.close()
                val bytesStream = new ByteArrayInputStream(bytes)
                
                val codec = Codec("UTF-8")
                codec.onMalformedInput(CodingErrorAction.IGNORE)
                codec.onUnmappableCharacter(CodingErrorAction.IGNORE)    
                val pulledString = scala.io.Source.fromInputStream(bytesStream)(codec).mkString
                
                if (pulledString.length > 1) { //utf-8 can be 4 bytes
                    lastFileIndex += pulledString.substring(0, pulledString.length).getBytes().length
                } else {
                    lastFileIndex += bytes.length
                }

                if (lastFileIndex + 1 >= objectSize) {
                    doneReading = true
                }

                currentString.append(pulledString)
            }
        }
        
        var beginTagIndex = currentString.indexOf(beginTag1, lastStrIndex)
        if (beginTagIndex < 0) {
            beginTagIndex = currentString.indexOf(beginTag2, lastStrIndex)
        }
        
        if (beginTagIndex >= 0) {
            val endTagIndex = currentString.indexOf(endTag, beginTagIndex)
            if (endTagIndex > 0) {
                val endIndex = endTagIndex + endTag.length
                val documentString = currentString.substring(beginTagIndex, endIndex)
                lastStrIndex = endIndex 
                return documentString
            } else if (!doneReading) {
                if (beginTagIndex > 0) {
                    currentString = currentString.takeRight(currentString.length - beginTagIndex)
                    return null
                }
            }
        } 
        
        currentString.clear()
        lastStrIndex = 0
        
        return null
    }
    
    def isDoneIterating(): Boolean = {
        return (doneReading && currentString.length <= 0)
    }
}

class S3ZSADirIterator(bucket: String, prefix: String = "", s3Client: AmazonS3 = getS3Client, endsWithFilters: Array[String] = Array(".xml", ".csv")) extends Serializable {
    private var listing: ObjectListing = null

    def getNextListing(): List[String] = {
        if (listing == null) {
            listing = s3Client.listObjects(bucket, prefix)
        } else {
            if (listing.isTruncated) {
                listing = s3Client.listNextBatchOfObjects(listing)
            } else {
                return Nil
            }
        }
        var keys = listing.getObjectSummaries.asScala.map(_.getKey).toList
        var results = ListBuffer[String]()
        endsWithFilters.foreach((filter) => {
            keys.foreach((key) => {
                if (key.toUpperCase().endsWith(filter.toUpperCase())) {
                    if (results.contains(key)) {
                        
                    } else {
                        results += key
                    }
                }
            })
        })
        return results.toList
    }
}

def pln(line: Any) = {
    val dt = LocalDateTime.now()
    println("(ZSA-" + dt.toString() + "): " + line.toString())
}

def delay() = {
    val seconds = 10
    pln("Waiting " + seconds.toString + " seconds...")
    Thread.sleep(seconds * 1000)
}

def getS3Client: AmazonS3 = {
    val awsKey = ""
    val awsAccessKey = ""
    val credentials = new BasicAWSCredentials(awsKey, awsAccessKey)
    return AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).build()
}

def getZSABucketName(): String = {
    return "data-zsa"
}

def getJSONWriteDir(): String = {
    return "JSON"
}

def getCSVWriteDir(): String = {
    return "CSV"
}

def getSchemaWriteDir(): String = {
    return "Schema"
}

def toJSON(obj: Any, depth: Int = 1) : String = {
    def tab() = "\t" * depth
    def tab2() = "\t" * (depth -1)
    
    var json = new ListBuffer[String]()
    obj match {
        case m: HashMap[_,_] => {
            for ( (k,v) <- m ) {
                var key = escape(k.asInstanceOf[String])
                v match {
                    case a: HashMap[_,_] => json += tab + "\"" + key + "\": " + toJSON(a, depth + 1)
                    case a: ListBuffer[_] => json += tab + "\"" + key + "\": " + toJSON(a, depth + 1)
                    case a: Array[_] => json += tab + "\"" + key + "\": " + toJSON(a, depth + 1)
                    case a: java.lang.Number => json += tab + "\"" + key + "\": " + a.toString()
                    case a: Boolean => json += tab + "\"" + key + "\": " + a
                    case a: String => json += tab + "\"" + key + "\": \"" + escape(a) + "\""
                    case _ => ;
                }
            }
        }
        case m: ListBuffer[_] => {
            var list = new ListBuffer[String]()
                for ( el <- m ) {
                    el match {
                        case a: HashMap[_,_] => list += tab + toJSON(a, depth + 1)
                        case a: ListBuffer[_] => list += tab + toJSON(a, depth + 1)
                        case a: Array[_] => list += tab + toJSON(a, depth + 1)
                        case a: java.lang.Number => list += tab + a.toString()
                        case a: Boolean => list += tab + a.toString()
                        case a: String => list += tab + "\"" + escape(a) + "\""
                        case _ => ;
                    }
                }
            return "[\n" + list.mkString(",\n") + "\n" + tab2 + "]"
        }
        case m: Array[_] => {
            var list = new ListBuffer[String]()
                for ( el <- m ) {
                    el match {
                        case a: HashMap[_,_] => list += tab + toJSON(a, depth + 1)
                        case a: ListBuffer[_] => list += tab + toJSON(a, depth + 1)
                        case a: Array[_] => list += tab + toJSON(a, depth + 1)
                        case a: java.lang.Number => list += tab + a.toString()
                        case a: Boolean => list += tab + a.toString()
                        case a: String => list += tab + "\"" + escape(a) + "\""
                        case _ => ;
                    }
                }
            return "[\n" + list.mkString(",\n") + "\n" + tab2 + "]"
        }
        case _ => ;
    }
    return "{\n" + json.mkString(",\n") + "\n" + tab2 + "}"
}

def escape(s: String) : String = {
    var str = s
    val b = "\\"
    val db = "\\" + "\\"
    str = str.replace(b, db)
    str = str.replace("\"" , "\\\"");
    str = str.replace("\n", "\\n")
    str = str.replace("\b", "\\b")
    str = str.replace("\f", "\\f")
    str = str.replace("\r", "\\r")
    str = str.replace("\t", "\\t")
    return str
}

def removeNamespaces(node: Node): Node = {
  node match {
    case elem: Elem => {
      elem.copy(
        scope = TopScope,
        prefix = null,
        attributes = removeNamespacesFromAttributes(elem.attributes),
        child = elem.child.map(removeNamespaces)
      )
    }
    case other => other
  }
}

def removeNamespacesFromAttributes(metadata: MetaData): MetaData = {
  metadata match {
    case UnprefixedAttribute(k, v, n) => new UnprefixedAttribute(k, v, removeNamespacesFromAttributes(n))
    case PrefixedAttribute(pre, k, v, n) => new UnprefixedAttribute(k, v, removeNamespacesFromAttributes(n))
    case Null => Null
  }
}

def isZSAAttributeNode(node: Node): Boolean = {
    if (node.child.length == 1) {
        if (node.child(0).isInstanceOf[Text]) {
            return true
        }
    }    
    return false
}

def buildZSAHashMap(node: Node): HashMap[String, Any] = {
    var map = new HashMap[String, Any]()
    node.child.foreach((node: Node) => {
        if (node.isInstanceOf[PCData] || node.isInstanceOf[Text]) {
        } else if (isZSAAttributeNode(node)) {
            map += (node.label -> node.text)
        } else {
            if (node.label == "LastUpdate") {
                buildZSAHashMap(node).foreach((x) => {
                    map += x
                })
            } else {
                if (map.contains(node.label)) {
                    
                } else {
                    map += (node.label -> new ListBuffer[HashMap[String, Any]])
                }
                var list = map(node.label).asInstanceOf[ListBuffer[HashMap[String, Any]]]
                list += buildZSAHashMap(node)
            }
        }
    })
    return map
}

def buildZSAMainHashMap(node: Node): HashMap[String, Any] = {
    var map = new HashMap[String, Any]()
    map += (node.label -> buildZSAHashMap(node))
    return map
}

def getDateTimeFormatter(str: String): DateTimeFormatter = {
    val dateFormatters = Array[DateTimeFormatter](
        DateTimeFormatter.ISO_LOCAL_DATE, //	ISO Local Date	'2011-12-03'
        DateTimeFormatter.ISO_LOCAL_DATE_TIME, //	ISO Local Date and Time	'2011-12-03T10:15:30'
        DateTimeFormatter.BASIC_ISO_DATE, //	Basic ISO date	'20111203'
        DateTimeFormatter.ISO_OFFSET_DATE, //	ISO Date with offset	'2011-12-03+01:00'
        DateTimeFormatter.ISO_DATE, //	ISO Date with or without offset	'2011-12-03+01:00'; '2011-12-03'
        DateTimeFormatter.ISO_LOCAL_TIME, //	Time without offset	'10:15:30'
        DateTimeFormatter.ISO_OFFSET_TIME, //	Time with offset	'10:15:30+01:00'
        DateTimeFormatter.ISO_TIME, //	Time with or without offset	'10:15:30+01:00'; '10:15:30'
        DateTimeFormatter.ISO_OFFSET_DATE_TIME, //	Date Time with Offset	2011-12-03T10:15:30+01:00'
        DateTimeFormatter.ISO_ZONED_DATE_TIME, //	Zoned Date Time	'2011-12-03T10:15:30+01:00[Europe/Paris]'
        DateTimeFormatter.ISO_DATE_TIME, //	Date and time with ZoneId	'2011-12-03T10:15:30+01:00[Europe/Paris]'
        DateTimeFormatter.ISO_ORDINAL_DATE, //	Year and day of year	'2012-337'
        DateTimeFormatter.ISO_WEEK_DATE, //	Year and Week	2012-W48-6'
        DateTimeFormatter.ISO_INSTANT, //	Date and Time of an Instant	'2011-12-03T10:15:30Z'
        DateTimeFormatter.RFC_1123_DATE_TIME
    )
    var index = 0
    while (index < dateFormatters.length) {
        val formatter = dateFormatters(index)
        try {
            val x = formatter.parse(str)
            return formatter
        } catch {
            case e: Exception => ;
        }
        index += 1
    }
    
    return null
}

def parseDateTime(str: String): LocalDateTime = {
    val formatter = getDateTimeFormatter(str)
    if (formatter == null) {
    } else {
        var ldt = LocalDateTime.now()
        try {
            ldt = LocalDateTime.parse(str, formatter)
            return ldt
        } catch {
            case e: Exception => {
                try {
                    val ld = LocalDate.parse(str, formatter)
                    return ld.atStartOfDay()
                } catch {
                    case e: Exception => {
                        try {
                            val lt = LocalTime.parse(str, formatter)
                            return lt.atDate(LocalDate.parse("1900-01-01"))
                        } catch {
                            case e: Exception => {
                                
                            }
                        }
                    }
                }
            }
        }
    }
    throw new Exception("Can't parse date with given parsers: " + str)
}

def isZSADate(str: String): Boolean = {
    try {
        val x = parseDateTime(str)
        return true
    } catch {
        case e: Exception => return false
    }
}

def isZSANumber(str: String): Boolean = {
    return str.matches("[-+]?\\d+(\\.\\d+)?")
}

def getTypeIDFromStr(typeStr: String): Long = {
    val types = List("Temporal", "Numeric", "Textual")
    return types.indexOf(typeStr)
}

def getTypeStrFromID(typeID: Long): String = {
    val types = List("Temporal", "Numeric", "Textual")
    return return types(typeID.toInt)
}

def getCQLTypeStrFromID(typeID: Long): String = {
    val types = List("timestamp", "decimal", "varchar")
    return return types(typeID.toInt)
}

def getTypeFromMap(value: HashMap[String, Long], columnName: String): Long = {
    var typeStr = "Textual"

    if (columnName.toUpperCase.endsWith("TEXT")) {
        typeStr = "Textual"
    } else {
        if (value("Temporal") > value("Numeric")) {
            if (value("Textual") > value("Temporal")) {
                typeStr = "Textual"
            } else {
                typeStr = "Temporal"
            }
        } else if (value("Numeric") > value("Textual")) {
            typeStr = "Numeric"
        } else {
            typeStr = "Textual"
        }
        
        if (typeStr == "Temporal") {
            if (value("Textual") > (value("Temporal") * 0.01)) {
                typeStr = "Textual"
            } else if (value("Numeric") > (value("Temporal") * 0.01)) {
                typeStr = "Numeric"
            }
        } else if (typeStr == "Numeric") {
            if (value("Textual") > (value("Numeric") * 0.01)) {
                typeStr = "Textual"
            }
        }
    }
    
    return getTypeIDFromStr(typeStr)
}

def mergeZSASchemaTables(toTable: ZSASchemaTable, fromTable: ZSASchemaTable): Int = {
    var merged = toTable
    for ((k, v) <- fromTable) {
        if (merged.contains(k)) {
            var mergedParent = merged(k)
            var parent = fromTable(k)
            for ((key, value) <- parent) {
                if (mergedParent.contains(key)) {
                    mergedParent(key)("Temporal") += value("Temporal")
                    mergedParent(key)("Numeric") += value("Numeric")
                    mergedParent(key)("Textual") += value("Textual")
                    mergedParent(key)("MaxLen") = if (mergedParent(key)("MaxLen") < value("MaxLen")) value("MaxLen") else mergedParent(key)("MaxLen")
                    mergedParent(key)("Type") = getTypeFromMap(mergedParent(key), key)
                } else {
                    mergedParent += (key -> value)
                }
            }
        } else {
            merged += (k -> v)
        }
    }
    return 0
}

def incrementFieldType(field: HashMap[String, Long], strVal: String): Int = {
    if (!field.contains("Type")) { field += ("Type" -> getTypeIDFromStr("Textual")) }
    if (!field.contains("Temporal")) { field += ("Temporal" -> 0) }
    if (!field.contains("Numeric")) { field += ("Numeric" -> 0) }
    if (!field.contains("Textual")) { field += ("Textual" -> 0) }
    if (!field.contains("MaxLen")) { field += ("MaxLen" -> strVal.length) }

    if (isZSADate(strVal)) {
        field("Temporal") += 1
    } else if (isZSANumber(strVal)) {
        field("Numeric") += 1
    } else {
        field("Textual") += 1
    }
    if (strVal.length > field("MaxLen")) {
        field("MaxLen") = strVal.length
    }
    return 0    
}

def buildZSASchemaFromZSATablesWithTypes(zsaTables: ZSATables, schemaTableRef: ZSASchemaTable): Int = {
    var schemaTable = schemaTableRef
    for ((key, rowList) <- zsaTables) {
        if (schemaTable.contains(key)) {
            
        } else {
            schemaTable += (key -> HashMap[String, HashMap[String, Long]]())
        }

        var parent = schemaTable(key)

        rowList.foreach((row) => {
            for ((fieldName, strVal) <- row) {
                if (parent.contains(fieldName)) {
                    
                } else {
                    parent += (fieldName -> HashMap[String, Long]())
                }
                var field = parent(fieldName).asInstanceOf[HashMap[String, Long]]
                incrementFieldType(field, strVal)
            }
        })
    }        
    return 0
}

def buildZSASchemaWithTypes(zsaMap: HashMap[String, Any], schemaTableRef: ZSASchemaTable, parentKey: String = ""): Int = {
    var schemaTable = schemaTableRef
    for ((key, value) <- zsaMap) {
        if (value.isInstanceOf[String]) {
            val strVal: String = value.asInstanceOf[String]
            var parent = schemaTable(parentKey)
            if (parent.contains(key)) {
                
            } else {
                parent += (key -> HashMap[String, Long]())
            }
            var field = parent(key).asInstanceOf[HashMap[String, Long]]
            incrementFieldType(field, strVal)
        } else {
            if (schemaTable.contains(key)) {
                
            } else {
                schemaTable += (key -> HashMap[String, HashMap[String, Long]]())
            }
            
            if (value.isInstanceOf[ListBuffer[_]]) {
                value.asInstanceOf[ListBuffer[HashMap[String, Any]]].foreach((x) => {
                    buildZSASchemaWithTypes(x.asInstanceOf[HashMap[String, Any]], schemaTable, key)                
                })
            } else {
                buildZSASchemaWithTypes(value.asInstanceOf[HashMap[String, Any]], schemaTable, key)
            }
        }
    }
    
    return 0
}

def mergeZSATables(toTable: ZSATables, fromTable: ZSATables): Int = {
    var merged = toTable
    for ((key, value) <- fromTable) {
        if (merged.contains(key)) {
            value.foreach((row) => {
                merged(key) += row
            })
        } else {
            merged += (key -> value)
        }
    }
    return 0
}

def getStringHashMapUUID(hashMap: HashMap[String, String]): UUID = {
    val sortedList = hashMap.toSeq.sortBy(_._1)
    var str = ""
    var index = 0
    while (index < sortedList.length) {
        str += (sortedList(index)._1 + sortedList(index)._2)
        index += 1
    }
    return UUID.nameUUIDFromBytes(str.getBytes())

}

def addIdentifiersToZSATables(zsaTablesRef: ZSATables, documentID: String): ZSATables = {
    var zsaTables = zsaTablesRef
    for ((tableName, table) <- zsaTables) {
        for ((row) <- table) {
            if (row.contains("DocumentID")) {
                
            } else {
                row += ("DocumentID" -> documentID)
                if (row.contains("ID")) { row.remove("ID") }
            }
            
            if (row.contains("ID")) {
                
            } else {
                row += ("ID" -> getStringHashMapUUID(row).toString())
            }
        }
    }
    return zsaTables
}

def buildZSATablesFromMap(zsaMap: HashMap[String, Any], zsaTablesRef: ZSATables, parentKey: String = ""): Int = {
    var zsaTables = zsaTablesRef
    var row = HashMap[String, String]()

    for ((xmlNodeName, value) <- zsaMap) {
        val key = fixColumnName(xmlNodeName)
        if (value.isInstanceOf[String]) {
            val strVal: String = value.asInstanceOf[String]
            row += (key -> strVal)
        } else {
            if (zsaTables.contains(key)) {
                
            } else {
                zsaTables += (key -> ListBuffer[HashMap[String, String]]())
            }
            
            if (value.isInstanceOf[ListBuffer[_]]) {
                value.asInstanceOf[ListBuffer[HashMap[String, Any]]].foreach((x) => {
                    buildZSATablesFromMap(x.asInstanceOf[HashMap[String, Any]], zsaTables, key)                
                })
            } else {
                buildZSATablesFromMap(value.asInstanceOf[HashMap[String, Any]], zsaTables, key)
            }
        }
    }
    
    if (parentKey.length > 0) {
        if (row.size > 0) {
            zsaTables(parentKey) += row
        }
    }
    
    return 0
}

def getZSADocumentIDFromAttachmentFileName(s3Key: String): String = {
    val fileName = getS3KeyFileName(s3Key)
    var id = "ZSA-"
    id = id + fileName.split('.')(3).split('_')(0)
    return id
}

def getZSADocumentIDFromHashMap(map: ZSAHashMap): String = {
    var id = "ZSA-"
    val mainMap = map("Main").asInstanceOf[ZSAHashMap]
    id = id + mainMap("ZSAID")
    return id
}

def getZSASchemaTableForJSON(table: ZSASchemaTable): HashMap[String, HashMap[String, HashMap[String, String]]] = {
    var merged = HashMap[String, HashMap[String, HashMap[String, String]]]()
    for ((key, value) <- table) {
        merged += (key -> HashMap[String, HashMap[String, String]]())
        var mergedParent = merged(key)
        var parent = value
        for ((key, value) <- parent) {
            val typeCounts = List("Textual: " + value("Textual").toString(), "Numeric: " + value("Numeric").toString(), "Temporal: " + value("Temporal").toString())
            mergedParent += (key -> HashMap[String, String]())
            mergedParent(key) += ("Type" -> getTypeStrFromID(value("Type")))
            mergedParent(key) += ("MaxLen" -> value("MaxLen").toString())
            mergedParent(key) += ("TypeCounts" -> typeCounts.toString())
        }
    }
    return merged
}

def mergeProcessResults(results: Array[ProcessResult], zsaSchemaTable: ZSASchemaTable = HashMap[String, ZSASchemaTableField]()): ProcessResult = {
    var totalDocumentsProcessed: Long = 0 //results.map(_.documentCount).sum
    var errors = ListBuffer[HashMap[String, String]]()
    results.foreach((result) => {
        mergeZSASchemaTables(zsaSchemaTable, result.zsaSchemaTable)
        result.errors.foreach((e) => {
            errors += e
        })
        totalDocumentsProcessed += result.documentCount
    })
    val result = new ProcessResult(totalDocumentsProcessed, zsaSchemaTable, errors)
    return result
}

def writeStringToS3(s3Client: AmazonS3, bucket: String, key: String, content: String, contentType: String = "plain/text") = {
    val bytes = content.getBytes("UTF-8")
    val inputStream = new ByteArrayInputStream(bytes)
    val metadata = new ObjectMetadata()
    metadata.setContentType(contentType)
    metadata.setContentLength(bytes.length)
    val request = new PutObjectRequest(bucket, key, inputStream, metadata)
    s3Client.putObject(request)
}

def writeHashMapAsJSONToS3(s3Client: AmazonS3, bucket: String, key: String, hashMap: HashMap[_, _]) = {
    val json = toJSON(hashMap)
    writeStringToS3(s3Client, bucket, key, json, "plain/json")
}

def readObjectFromS3(s3Client: AmazonS3, bucket: String, key: String): Any = {
    val objRequest = new GetObjectRequest(bucket, key)
    val stream = s3Client.getObject(objRequest).getObjectContent
    val objInputStream = new ObjectInputStream(stream)
    val obj = objInputStream.readObject()
    objInputStream.close()
    stream.close()
    return obj
}

def getS3Str(s3Client: AmazonS3, bucket: String, key: String, encoding: String = "UTF-8", fix: Boolean = false): String = {
    val objRequest = new GetObjectRequest(bucket, key)
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

def writeObjectToS3(s3Client: AmazonS3, bucket: String, key: String, obj: Serializable) = {
    val byteStream = new ByteArrayOutputStream()
    val objStream = new ObjectOutputStream(byteStream)
    objStream.writeObject(obj)
    objStream.close()
    byteStream.close()
    val byteArray = byteStream.toByteArray()
    val metadata = new ObjectMetadata()
    metadata.setContentType("application/binary")
    metadata.setContentLength(byteArray.length)
    val request = new PutObjectRequest(bucket, key, new ByteArrayInputStream(byteArray), metadata)
    s3Client.putObject(request)
}

def formatDSECSVValue(valStr: String, typeStr: String): String = {
    var retVal = valStr
    
    if (valStr == null) {
        retVal = ""    
    } else {
        if (typeStr == "Textual") {
            retVal = "\"" + retVal.replace("\"", "\"\"").replace("\r\n", "\n").replace("\n", "\\n") + "\""
        } else if (typeStr == "Temporal") {
            try {
                retVal = parseDateTime(retVal).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            } catch {
                case e: Exception => {
                    retVal = ""
                }
            }
        } else if (typeStr == "Numeric") {
            if (isZSANumber(retVal)) {

            } else {
                retVal = ""
            }
        }
    }
    
    return retVal
}

def getCSVStrForZSATable(zsaTable: ZSATable, zsaSchemaTableField: ZSASchemaTableField, includeHeaders: Boolean = true): String = {
    val sortedFields = zsaSchemaTableField.toSeq.sortBy(_._1)
    var rowIndex = 0
    var csvStr = new StringBuilder(10000)
    var colIndex = 0
    val newLine = "\n"
    var fields = ListBuffer[String]()

    var index = 0
    fields += "ID"
    fields += "DocumentID"
    while (index < sortedFields.size) {
        fields += sortedFields(index)._1
        index += 1
    }

    if (fields.size > 0) {
        if (includeHeaders) {
            while (colIndex < fields.size) {
                csvStr.append(fields(colIndex))
                if (colIndex + 1 < fields.size) {
                    csvStr.append(",")
                }
                colIndex += 1
            }
    
            csvStr.append(newLine)
        }

        while (rowIndex < zsaTable.size) {
            colIndex = 0
            val row = zsaTable(rowIndex)    
            
            while (colIndex < fields.size) {
                val col = fields(colIndex)
                if (row.contains(col)) {
                    if (col == "ID") {
                        csvStr.append(row(col))
                    } else if (col == "DocumentID") {
                        csvStr.append(row(col))
                    } else {
                        csvStr.append(formatDSECSVValue(row(col), getTypeStrFromID(zsaSchemaTableField(col)("Type"))))
                    }
                } 

                if (colIndex + 1 < fields.size) {
                    csvStr.append(",")
                }
                
                colIndex += 1
            }
            
            rowIndex += 1

            csvStr.append(newLine)
        }
    }
    
    return csvStr.toString()
}

def writeCSVZSATable(s3Client: AmazonS3, bucket: String, key: String, zsaTable: ZSATable, zsaSchemaTableField: ZSASchemaTableField) = {
    val csvStr = getCSVStrForZSATable(zsaTable, zsaSchemaTableField, true)
    if (zsaTable.size > 0) {
        writeStringToS3(s3Client, bucket, key, csvStr, "text/csv")
    }
}

def writeZSATablesAsCSV(s3Client: AmazonS3, bucket: String, csvKey: String, zsaTables: ZSATables, zsaSchemaTable: ZSASchemaTable) = {
    val batchID = UUID.randomUUID().toString()
    for ((key, value) <- zsaTables) {
        val s3key = csvKey + key + "/Parts/" + key + "-" + batchID + ".csv"
        writeCSVZSATable(s3Client, getZSABucketName, s3key, value, zsaSchemaTable(key))
    }
}

def getCQLScriptFromZSASchema(keyspace: String, zsaSchemaTable: ZSASchemaTable): Array[String] = {
    val newLine = "\n"

    var build = new StringBuilder("")
    var delete = new StringBuilder("")
    
    build.append("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'TimT' : 3 };" + newLine)
    build.append(newLine)
    build.append("USE " + keyspace + ";")
    build.append(newLine)
    
    delete.insert(0, newLine + "DROP KEYSPACE IF EXISTS " + keyspace + ";")

    for ((key, value) <- zsaSchemaTable) {
        build.append(newLine)
        build.append("CREATE TABLE IF NOT EXISTS " + keyspace + ".\"" + key + "\" (\"ID\" uuid, \"DocumentID\" varchar, PRIMARY KEY (\"ID\", \"DocumentID\"));" + newLine)

        for ((k, v) <- value) {
            build.append("ALTER TABLE " + keyspace + ".\"" + key + "\" ADD \"" + k + "\" " + getCQLTypeStrFromID(v("Type")) + ";" + newLine)
        }

        delete.insert(0, "DROP TABLE IF EXISTS " + keyspace + ".\"" + key + "\";\n")
    }
    
    return Array(build.toString(), delete.toString())
}

def resetZSASchemaTableTypes(zsaSchemaTable: ZSASchemaTable): ZSASchemaTable = {
    for ((key, value) <- zsaSchemaTable) {
        for ((k, v) <- value) {
            v("Temportal") = 0
            v("Numeric") = 0
            v("Textual") = 0
            v("Type") = getTypeIDFromStr("Textual")
            v("MaxLen") = 0
        }
    }
    return zsaSchemaTable
}

def doZSATablesMeetThreshold(zsaTables: ZSATables, documentThreshold: Long, recordThresholdPerTable: Long): Boolean = {
    if (zsaTables.contains("Main")) {
        if (zsaTables("Main").size > documentThreshold) {
            return true
        }
    }
    
    for ((key, table) <- zsaTables) {
        if (table.size > recordThresholdPerTable) {
            return true
        }
    }
    
    return false
}

def isZSADocument(s3Key: String): Boolean = {
    val fileName = getS3KeyFileName(s3Key)
    if (fileName.length > 0) {
        val ext = getFileExtension(fileName)
        if (ext.toUpperCase() == "XML") {
            return true
        }
    }
    return false
}

def isZSAAttachment(s3Key: String): Boolean = {
    val fileName = getS3KeyFileName(s3Key)
    if (fileName.length > 0) {
        val ext = getFileExtension(fileName)
        if (ext.toUpperCase() == "CSV" && fileName.contains("Attachment")) {
            return true
        }
    }
    return false
}

def isZSAFile(s3Key: String): Boolean = {
    return isZSADocument(s3Key) || isZSAAttachment(s3Key)
}

def randNo(start: Int, end: Int): Int = {
    val random = new scala.util.Random
    val result = start + random.nextInt( (end - start) + 1 )
    return result
}

def handleError(e: Exception, errors: ListBuffer[HashMap[String, String]], s3Key: String, documentCounter: Long) = {
    val error = HashMap[String, String]()
    error += ("Message" -> e.toString)
    error += ("Type" -> e.getClass.toString)
    error += ("S3Key" -> s3Key)
    error += ("DocumentCounter" -> documentCounter.toString)
    errors += error
    pln(e)
}

def process(s3Dirs: ListBuffer[String], paralellize: Boolean = true, partitions: Int = 100, s3KeyBatchSize: Long = 1000, aggregateCSV: Boolean = true, writeJSON: Boolean = true, writeCSV: Boolean = true, writeSchema: Boolean = true, maxDocuments: Long = Long.MaxValue, maxS3Keys: Long = Long.MaxValue, maxDocumentsPerS3Key: Long = Long.MaxValue, csvDocumentBatchSize: Long = 1000, zsaSchemaTable: ZSASchemaTable = HashMap[String, ZSASchemaTableField](), keySpace: String = "zsa_raw_data", writeS3KeyPrefix: String = "99-Testing/", writeS3KeyPostfix: String = processStart.toString() + "/", oneTypeAtATime: Boolean = true, onlyAggregate: Boolean = false, entityToAggregate: String = ""): ProcessResult = {
    
    var totalDocumentsProcessed: Long = 0
    var processResult = new ProcessResult()
    
    processResult.zsaSchemaTable = zsaSchemaTable

    if (onlyAggregate) {
        processResult.loadScript = aggregateAllCSVs(zsaSchemaTable, paralellize, writeS3KeyPrefix, writeS3KeyPostfix, keySpace, entityToAggregate)
        return processResult
    }
    
    pln("Building and writing S3 job results delete script...")
    var deleteScript = buildDeleteScript(getZSABucketName, writeS3KeyPrefix, writeS3KeyPostfix)
    writeAndShowCommandScript(getZSABucketName, deleteScript, "DeleteJobResultsScript.sh", writeS3KeyPrefix, writeS3KeyPostfix)
    pln("Done building and writing S3 job results delete script.")
    
    def handleBatch(batchKeys: ListBuffer[String]): ProcessResult = {
        var results = Array[ProcessResult]()
        
        pln("Handling batch of " + batchKeys.length.toString() + " S3 keys...")

        def doWork(s3Key: String): ProcessResult = {
            var result = new ProcessResult()
            
            if (totalDocumentsProcessed < maxDocuments) {
                var zsaTables = HashMap[String, ZSATable]()
                var documentCounter: Long = 0
                val s3Client = getS3Client
                var errors = ListBuffer[HashMap[String, String]]()

                pln("Handling '" + s3Key + "'...")
                
                if (isZSADocument(s3Key)) {
                    val extractor = new ZSAXMLMainDocumentStringIterator(getZSABucketName(), s3Key)

                    while (!extractor.isDoneIterating() && documentCounter < maxDocumentsPerS3Key && documentCounter + totalDocumentsProcessed < maxDocuments && errors.length < 10) {
                        try {
                            val xmlStr = extractor.getNextDocument()
                            if (xmlStr == null) {
                                
                            } else {
                                documentCounter += 1
                                totalDocumentsProcessed += 1

                                var xml: Node = XML.load(Source.fromString(xmlStr))
                                xml = removeNamespaces(xml)
    
                                var zsaMap = buildZSAMainHashMap(xml)
                                val documentID = getZSADocumentIDFromHashMap(zsaMap)
    
                                var newZSASchemaTable = HashMap[String, ZSASchemaTableField]()
                                buildZSASchemaWithTypes(zsaMap, newZSASchemaTable)
                                mergeZSASchemaTables(zsaSchemaTable, newZSASchemaTable)
        
                                if (writeJSON) {
                                    val s3Key = writeS3KeyPrefix + getJSONWriteDir() + "/" + writeS3KeyPostfix + documentID + ".json"
                                    writeHashMapAsJSONToS3(s3Client, getZSABucketName(), s3Key, zsaMap)
                                }
                                
                                if (writeCSV) {
                                    var zsaTablesTemp = HashMap[String, ZSATable]() //so that we aren't always scanning the table for ID and DocumentID fields (identifiers)
                                    buildZSATablesFromMap(zsaMap, zsaTablesTemp)
                                    addIdentifiersToZSATables(zsaTablesTemp, documentID)
                                    mergeZSATables(zsaTables, zsaTablesTemp)
                                    if (doZSATablesMeetThreshold(zsaTables, csvDocumentBatchSize, csvDocumentBatchSize * 100)) {
                                        val s3Key = writeS3KeyPrefix + getCSVWriteDir() + "/" + writeS3KeyPostfix
                                        writeZSATablesAsCSV(s3Client, getZSABucketName, s3Key, zsaTables, zsaSchemaTable)
                                        zsaTables = HashMap[String, ZSATable]()
                                    }
                                }
                            }
                        } catch {
                            case e: Exception => {
                                handleError(e, errors, s3Key, documentCounter)
                                delay()
                            }
                        }
                    }
                } else if (isZSAAttachment(s3Key)) {
                    var zsaTablesTemp = HashMap[String, ZSATable]() //so that we aren't always scanning the table for ID and DocumentID fields (identifiers)
                    val documentID = getZSADocumentIDFromAttachmentFileName(s3Key)

                    documentCounter += 1
                    totalDocumentsProcessed += 1
                    
                    try {
                        val csvStr = getS3Str(s3Client, getZSABucketName, s3Key)
                        val zsaTableTemp = getZSATableFromCSVStr(csvStr)
                        var zsaTable = ListBuffer[HashMap[String, String]]()
                        var rowIndex = 0
                        while (rowIndex < zsaTableTemp.size) {
                            val sourceRow = zsaTableTemp(rowIndex)
                            var destRow = HashMap[String, String]()
                            val jsonStr = toJSON(sourceRow).replace("\n", "").replace("\t", "")
                            destRow += ("RowIndex" -> rowIndex.toString())
                            destRow += ("RowData" -> jsonStr)
                            zsaTable += destRow
                            rowIndex += 1
                        }
                        zsaTablesTemp += ("Attachments" -> zsaTable)
                        
                        var newZSASchemaTable = HashMap[String, ZSASchemaTableField]()
                        buildZSASchemaFromZSATablesWithTypes(zsaTablesTemp, newZSASchemaTable)
                        mergeZSASchemaTables(zsaSchemaTable, newZSASchemaTable)
    
                        addIdentifiersToZSATables(zsaTablesTemp, documentID)
                        mergeZSATables(zsaTables, zsaTablesTemp)
                    } catch {
                        case e: Exception => {
                            handleError(e, errors, s3Key, documentCounter)
                            delay()
                        }
                    }
                }
                
                if (writeCSV) {
                    if (doZSATablesMeetThreshold(zsaTables, 0, 0)) {
                        val s3Key = writeS3KeyPrefix + getCSVWriteDir() + "/" + writeS3KeyPostfix
                        try {
                            writeZSATablesAsCSV(s3Client, getZSABucketName, s3Key, zsaTables, zsaSchemaTable)
                            zsaTables = HashMap[String, ZSATable]()
                        } catch {
                            case e: Exception => {
                                handleError(e, errors, s3Key, documentCounter)
                            }
                        }
                    }
                }
                
                result = new ProcessResult(documentCounter, zsaSchemaTable, errors)

                pln("Done handling '" + s3Key + "'.")
            }

            return result
        }

        if (paralellize) {
            var xmlKeyList = sc.parallelize(batchKeys, partitions)   
            pln("Running parallelized. Partitions: " + xmlKeyList.getNumPartitions + ".")
            val mapResults = xmlKeyList.map((s3Key) => {
                val result = doWork(s3Key)
                result
            })
            results = mapResults.collect().toArray
        } else {
            var xmlKeyList = batchKeys
            pln("Running serialized. No paralellization.")
            val mapResults = xmlKeyList.map((s3Key) => {
                val result = doWork(s3Key)
                result
            })
            results = mapResults.toArray
        }

        val result = mergeProcessResults(results)

        pln("Done handling batch of " + batchKeys.length.toString() + " S3 keys.")
        
        return result
    }

    if (maxS3Keys > 0 && maxDocumentsPerS3Key > 0 && maxDocuments > 0 && s3Dirs.size > 0) {
        val results = new ListBuffer[ProcessResult]()
        val fileTypes = Array(".csv", ".xml")
        var documentsPerParentKey = (maxDocuments / s3Dirs.size) + 1
        var documentsPerFileType = (documentsPerParentKey / fileTypes.length) + 1
        var keyCounter: Long = 0
        var skippedKeys: Long = 0
        var finished = false
        
        if (maxDocuments == Long.MaxValue) { 
            documentsPerParentKey = Long.MaxValue
            documentsPerFileType = Long.MaxValue
        }
        
        pln("Documents per parent key: " + documentsPerParentKey.toString() + ". " + "Documents per file type: " + documentsPerFileType.toString() + ".")

        var batchKeys: ListBuffer[String] = new ListBuffer[String]()
        s3Dirs.foreach((s3Dir) => {
            if (!finished) {
                pln("Loading keys for '" + s3Dir + "'...")
                var parentDocumentCounter: Long = 0
                fileTypes.foreach((fileType) => {
                    if (!finished) {
                        pln("Loading keys for " + fileType + "...")
                        var typeDocumentCounter: Long = 0
                        val listing = new S3ZSADirIterator(getZSABucketName(), s3Dir, getS3Client, Array(fileType))
                        var keys = listing.getNextListing()
                        var keyIndex: Int = 0
                        var listingFinished = false
                        
                        while ((keys.length > 0) && (!finished) && (!listingFinished) && (parentDocumentCounter < documentsPerParentKey) && (typeDocumentCounter < documentsPerFileType)) {
                            val key = keys(keyIndex)

                            if (isZSAFile(key)) {
                                batchKeys += key
                                keyCounter += 1
                            } else {
                                skippedKeys += 1
                            }

                            if (batchKeys.length >= s3KeyBatchSize) {
                                val result = handleBatch(batchKeys)
                                if (paralellize) {
                                    totalDocumentsProcessed += result.documentCount
                                }
                                parentDocumentCounter += result.documentCount
                                typeDocumentCounter += result.documentCount

                                results += result
                                pln("Batch document count: " + result.documentCount.toString + "; Total: " + totalDocumentsProcessed.toString + "; Key count: " + keyCounter.toString + "; Skipped keys: " + skippedKeys.toString + ".")

                                batchKeys = new ListBuffer[String]()
                            }
        
                            if (keyCounter < maxS3Keys && totalDocumentsProcessed < maxDocuments) {
                                if (keyIndex < keys.length - 1) {
                                    keyIndex += 1
                                } else {
                                    keys = listing.getNextListing()
                                    keyIndex = 0
                                    if (keys.size <= 0) {
                                        listingFinished = true
                                    }
                                }
                            } else {
                                finished = true
                            }
                        }
            
                        if (batchKeys.length > 0 && totalDocumentsProcessed < maxDocuments && oneTypeAtATime) {
                            val result = handleBatch(batchKeys)
                            if (paralellize) {
                                totalDocumentsProcessed += result.documentCount
                            }
                            parentDocumentCounter += result.documentCount
                            typeDocumentCounter += result.documentCount

                            results += result
                            pln("Batch document count: " + result.documentCount.toString + "; Total: " + totalDocumentsProcessed.toString + "; Key count: " + keyCounter.toString + "; Skipped keys: " + skippedKeys.toString + ".")
                            
                            batchKeys = new ListBuffer[String]()
                        }
                        pln("Done loading keys for " + fileType + ".")
                    }
                })
                pln("Done loading keys for '" + s3Dir + "'.")
            }
        })

        if (batchKeys.length > 0 && totalDocumentsProcessed < maxDocuments) {
            val result = handleBatch(batchKeys)
            if (paralellize) {
                totalDocumentsProcessed += result.documentCount
            }

            results += result
            pln("Batch document count: " + result.documentCount.toString + "; Total: " + totalDocumentsProcessed.toString + "; Key count: " + keyCounter.toString + "; Skipped keys: " + skippedKeys.toString + ".")
            
            batchKeys = new ListBuffer[String]()
        }
        
        processResult = mergeProcessResults(results.toArray, processResult.zsaSchemaTable)

        if (writeSchema) {
            var s3Key = ""

            s3Key = writeS3KeyPrefix + getSchemaWriteDir() + "/" + writeS3KeyPostfix + "ZSATablesSchema.json"
            pln("Writing schema JSON to '" + getZSABucketName + "/" + s3Key + "'...")
            writeHashMapAsJSONToS3(getS3Client, getZSABucketName, s3Key, getZSASchemaTableForJSON(processResult.zsaSchemaTable))

            s3Key = writeS3KeyPrefix + getSchemaWriteDir() + "/" + writeS3KeyPostfix + "ZSATablesSchema.obj"
            pln("Writing schema object to '" + getZSABucketName + "/" + s3Key + "'...")
            writeObjectToS3(getS3Client, getZSABucketName, s3Key, processResult.zsaSchemaTable)

            s3Key = writeS3KeyPrefix + getSchemaWriteDir() + "/" + writeS3KeyPostfix + "ZSATablesSchema-Create.cql"
            val cql = getCQLScriptFromZSASchema(keySpace, processResult.zsaSchemaTable)
            pln("Writing CQL schema script to '" + getZSABucketName + "/" + s3Key + "'...")
            writeStringToS3(getS3Client, getZSABucketName, s3Key, cql(0))

            s3Key = writeS3KeyPrefix + getSchemaWriteDir() + "/" + writeS3KeyPostfix + "ZSATablesSchema-Delete.cql"
            pln("Writing CQL schema script to '" + getZSABucketName + "/" + s3Key + "'...")
            writeStringToS3(getS3Client, getZSABucketName, s3Key, cql(1))

            pln("Done writing schema.")
        }
    
        if (writeCSV && aggregateCSV) {
            processResult.loadScript = aggregateAllCSVs(processResult.zsaSchemaTable, paralellize, writeS3KeyPrefix, writeS3KeyPostfix, keySpace, entityToAggregate)
        }
    }
    
    return processResult
}

def aggregateAllCSVs(zsaSchemaTable: ZSASchemaTable, paralellize: Boolean, writeS3KeyPrefix: String, writeS3KeyPostfix: String, keySpace: String, entityToAggregate: String = ""): String = {
    var tableNames = zsaSchemaTable.keys.toSeq.toList
    var s3Key = writeS3KeyPrefix + getCSVWriteDir() + "/" + writeS3KeyPostfix
    var loadScriptPath = "s3://" + getZSABucketName + "/" + writeS3KeyPrefix + getSchemaWriteDir + "/" + writeS3KeyPostfix + "ZSATablesLoadFromCSVScript.sh"
    
    if (entityToAggregate.length > 0) {
        tableNames = List(entityToAggregate)
    }
    
    pln("Building and writing load script...")
    var loadScript = buildLoadScript(getZSABucketName, writeS3KeyPrefix, writeS3KeyPostfix, keySpace, zsaSchemaTable)
    writeAndShowCommandScript(getZSABucketName, loadScript, "ZSATablesLoadFromCSVScript.sh", writeS3KeyPrefix, writeS3KeyPostfix)
    pln("Done building and writing load script.")

    pln("Aggregating CSVs...")
    if (tableNames.length > 0) {
        if (paralellize) {
            var tableList = sc.parallelize(tableNames, tableNames.length)   
            pln("Running parallelized. Partitions: " + tableList.getNumPartitions + ".")
            tableList.foreach((key) => {
                aggregateCSVs(s3Key, key, zsaSchemaTable(key))            
            })
        } else {
            var tableList = tableNames
            pln("Running serialized. No paralellization.")
            tableList.foreach((key) => {
                aggregateCSVs(s3Key, key, zsaSchemaTable(key))            
            })
        }
    }
    pln("Done aggregating CSVs.")
    
    return loadScriptPath
}

def aggregateCSVs(s3Key: String, tableName: String, zsaSchemaTableField: ZSASchemaTableField): Int = {
    val s3Client = getS3Client
    val listing = new S3ZSADirIterator(getZSABucketName(), s3Key + tableName + "/Parts", getS3Client, Array(".csv"))
    //val listing = new S3ZSADirIterator(getZSABucketName(), s3Key + tableName + "/Parts/Attachments-26760c1f-1abc-4e1f-8aab-b16a246cb50f.csv", getS3Client, Array(".csv"))
    var keyCount = 0
    var wroteHeader = false
    val writeFilePath = "/data/" + tableName + ".csv"
    val writeFile = new File(writeFilePath)
    var totalIndex = 0

    var keys = listing.getNextListing()

    if (keys.length > 0) {
        pln("Aggregating CSVs for " + s3Key + tableName + "...")

        var keyIndex: Int = 0
        
        while (keyIndex < keys.length) {
            val key = keys(keyIndex)
            val keyIndexStr = "Key index: " + totalIndex.toString() + "; "
            keyCount += 1
            totalIndex += 1

            pln(keyIndexStr + "Combining " + key + "...")

            val csvStr = getS3Str(s3Client, getZSABucketName, key, fix = true)
            
            pln(keyIndexStr + "Loaded " + key + "...")
            
            var zsaTable = getZSATableFromCSVStr(csvStr)

            pln(keyIndexStr + "Converted " + key + " to table.")
            
            if (wroteHeader) {
                val csvStr = getCSVStrForZSATable(zsaTable, zsaSchemaTableField, false)
                val fileWriter = new FileWriter(writeFile, true)
                fileWriter.write(csvStr)
                fileWriter.close()
            } else {
                val csvStr = getCSVStrForZSATable(zsaTable, zsaSchemaTableField, true)
                val fileWriter = new FileWriter(writeFile, false)
                fileWriter.write(csvStr)
                fileWriter.close()
                wroteHeader = true
            }
            
            pln(keyIndexStr + "Done combining " + key + ".")
            
            if (keyIndex < keys.length - 1) {
                keyIndex += 1
            } else {
                keys = listing.getNextListing()
                keyIndex = 0
            }
        }
        
        pln("Done combining all for " + s3Key + tableName + ".")
        
        val s3WritePath = s3Key + tableName + "/" + tableName + ".csv"
        doMultiPartUpload(getZSABucketName, s3WritePath, writeFile)
        writeFile.delete()

        pln("Done aggregating CSVs for " + s3Key + tableName + ". File count: " + keyCount.toString + ".")
    }        
    
    return 0
}

def writeAndShowCommandScript(bucket: String, script: String, scriptName: String, writeS3KeyPrefix: String, writeS3KeyPostfix: String): Unit = {
    var s3Key = writeS3KeyPrefix + getSchemaWriteDir() + "/" + writeS3KeyPostfix + scriptName
    writeStringToS3(getS3Client, getZSABucketName, s3Key, script)
    pln("Commands to run " + scriptName + " script:")
    pln("\taws s3 cp \"s3://" + getZSABucketName + "/" + s3Key + "\" . --region us-x-west-1")
    pln("\tchmod +x ./" + scriptName)
    pln("\t./" + scriptName)
}

def getShellScriptHeader(): StringBuilder = {
    var script = new StringBuilder()
    val newLine = "\n"
    
    script.append("#!/bin/bash -vx" + newLine)    
    script.append(newLine)
    script.append("#fail all if one fails" + newLine)
    script.append("set -e" + newLine)
    script.append(newLine)
    
    return script
}

def buildLoadScript(bucket: String, writeS3KeyPrefix: String, writeS3KeyPostfix: String, keySpace: String, zsaSchemaTable: ZSASchemaTable): String = {
    val s3SchemaKey = writeS3KeyPrefix + getSchemaWriteDir() + "/" + writeS3KeyPostfix
    val s3CSVKey = writeS3KeyPrefix + getCSVWriteDir() + "/" + writeS3KeyPostfix
    val tables = zsaSchemaTable.keys.toSeq.toList
    var script = new StringBuilder()
    val newLine = "\n"
    val s3Client = getS3Client
    
    script.append(getShellScriptHeader)    
    script.append("aws s3 cp \"s3://" + bucket + "/" + s3SchemaKey + "ZSATablesSchema-Create.cql\" . --region us-x-west-1" + newLine)
    script.append("aws s3 cp \"s3://" + bucket + "/" + s3SchemaKey + "ZSATablesSchema-Delete.cql\" . --region us-x-west-1" + newLine)
    script.append("aws s3 cp \"s3://" + bucket + "/" + s3SchemaKey + "DeleteJobResultsScript.sh\" . --region us-x-west-1" + newLine)
    script.append("chmod +x DeleteJobResultsScript.sh" + newLine)
    script.append(newLine)
    script.append("cqlsh --debug -f ZSATablesSchema-Delete.cql x.x.x.10" + newLine)
    script.append("cqlsh --debug -f ZSATablesSchema-Create.cql x.x.x.10" + newLine)
    script.append(newLine)
    
    tables.foreach((table) => {
        val objKey = s3CSVKey + table + "/" + table + ".csv"
        if (s3Client.doesObjectExist(bucket, objKey)) {
            script.append("aws s3 cp \"s3://" + bucket + "/" + objKey + "\" --region us-x-west-1 " + table + ".csv" + newLine)
            script.append("dsbulk load -h 'x.x.x.10, x.x.x.169, x.x.x.125'")
            script.append(" -url \"" + table + ".csv\" -k " + keySpace + " -t " + table + " --connector.csv.maxCharsPerColumn -1 -escape '\\\"'" + newLine)
            script.append("rm -f " + table + ".csv" + newLine)
            script.append(newLine)
        }
    })
    
    return script.toString
}


def buildDeleteScript(bucket: String, writeS3KeyPrefix: String, writeS3KeyPostfix: String): String = {
    var script = new StringBuilder()
    val newLine = "\n"

    script.append(getShellScriptHeader) 
    script.append("yum install multitail -y" + newLine)
    script.append("multitail -z")
    script.append(" -l \"aws s3 rm 's3://" + bucket + "/" + writeS3KeyPrefix + getJSONWriteDir + "/" + writeS3KeyPostfix + "' --recursive --region us-x-west-1\"")    
    script.append(" -l \"aws s3 rm 's3://" + bucket + "/" + writeS3KeyPrefix + getCSVWriteDir + "/" + writeS3KeyPostfix + "' --recursive --region us-x-west-1\"")    
    script.append(" -l \"aws s3 rm 's3://" + bucket + "/" + writeS3KeyPrefix + getSchemaWriteDir + "/" + writeS3KeyPostfix + "' --recursive --region us-x-west-1\"")    
    script.append(newLine)
    
    return script.toString
}

def fixColumnName(colName: String): String = {
    val validChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890_"
    val replaceWith = ""
    var col = colName
    val colArr = col.toCharArray()
    var colIndex = 0
    while (colIndex < colArr.length) {
        val c = colArr(colIndex)
        if (validChars.contains(c)) {
            
        } else {
            col = col.replace(c.toString(), replaceWith)
        }
        colIndex += 1
    }
    return col
}

def getZSATableFromCSVStr(csvStr: String): ZSATable = {
    val rows = parseCSVStrRows(csvStr)
    var zsaTable = ListBuffer[HashMap[String, String]]()
    
    if (rows.length > 0) {
        var columns = ListBuffer[String]()
        var rowIndex = 0
        var validHeader = false
    
        while (!validHeader && rowIndex < rows.length) {
            columns = rows(rowIndex)
            var colIndex = 0
            var foundEmpty = false
            while (colIndex < columns.length && !foundEmpty) {
                columns(colIndex) = fixColumnName(columns(colIndex))
                if (columns(colIndex).length > 0) {
                    
                } else {
                    foundEmpty = true
                }
                colIndex += 1
            }
            validHeader = !foundEmpty
            rowIndex += 1
        }

        while (rowIndex < rows.length) {
            val row = rows(rowIndex)
            var tableRow = HashMap[String, String]()
            var colIndex = 0
            var foundVal = false
            while (colIndex < columns.length) {
                if (colIndex < row.length) {
                    val col = columns(colIndex)
                    val colVal = row(colIndex)
                    tableRow += (col -> colVal)
                    if (colVal.length > 0) {
                        foundVal = true
                    }
                }
                colIndex += 1
            }
            if (foundVal) {
                zsaTable += tableRow
            }
            rowIndex += 1
        }
    }
    
    return zsaTable
}

def parseCSVStrRows(csvStr: String): ListBuffer[ListBuffer[String]] = {
    var results = ListBuffer[ListBuffer[String]]()
    val quote = "\"".toCharArray()(0)
    val sep = ','
    val backslash = '\\'
    var csvStrMod = csvStr.replace("\r\n", "\n")
    val chars = csvStrMod.toCharArray();
    var curVal = new StringBuilder();
    var charIndex = 0

    while (charIndex < chars.length) {
        var inQuotes: Boolean = false;
        var result = ListBuffer[String]()
        var finished = false
        curVal.clear()

        while (charIndex < chars.length && !finished) {
            val ch = chars(charIndex)
            if (ch == 0) {
                
            } else {
                if (inQuotes) {
                    if (ch == quote && (charIndex + 1 < chars.length) && chars(charIndex + 1) == quote) {
                        curVal.append("\"")
                        charIndex += 1
                    } else if (ch == quote) {
                        inQuotes = false;
                    } else {
                        curVal.append(ch)
                    }
                } else {
                    if (ch == quote) {
                        inQuotes = true;
                    } else if (ch == sep) {
                        result += curVal.toString
                        curVal.clear()
                    } else if (ch == '\r') {
                    } else if (ch == '\n') {
                        finished = true
                    } else {
                        curVal.append(ch);
                    }
                }
            }
            
            charIndex += 1
        }
    
        result += curVal.toString

        results += result
    }

    return results;    
}

def doMultiPartUpload(bucket: String, key: String, f: File) = {
    var xfer_mgr = TransferManagerBuilder.standard().withS3Client(getS3Client).build()
    var xfer = xfer_mgr.upload(bucket, key, f)
    xfer.waitForCompletion()
    xfer_mgr.shutdownNow()
}

def formatDuration(diff: Duration): String = {
    val s = duration.getSeconds() + 1
    return "%d:%02d:%02d".format(s / 3600, (s % 3600) / 60, (s % 60))    
}

def getZSASchemaFromS3(): ZSASchemaTable = {
    val schemaFile = "SeedZSATablesSchema.obj"
    val zsaSchema = readObjectFromS3(getS3Client, "timt-dev", schemaFile).asInstanceOf[ZSASchemaTable]
    return zsaSchema
}

def startShellProcess(cmdLine: String): Int = {
    pln("Running command line: " + cmdLine + "...")
    var pb = new ProcessBuilder("bash", "-c", cmdLine)
    pb.redirectErrorStream(true);
    var p = pb.start()
    var ir = new BufferedReader(new InputStreamReader(p.getInputStream()))
    var line = ""
    var counter = 0
    val maxLines = 10000
    while (counter < maxLines) {
        line = ir.readLine()
        if (line != null) {
            pln(line)
        } else {
            counter = maxLines
        }
        counter += 1
    }
    ir.close()
    var retVal = p.waitFor()
    pln("Done running command line. Result: " + retVal.toString + ".")
    return retVal
}

def runS3JobScript(s3URL: String): Int = {
    val scriptName = getS3KeyFileName(s3URL)
    val cmd = "export PATH=/dsbulk-1.5.0/bin:/cqlsh-6.8.0/bin:$PATH && cd /data && aws s3 cp \"" + s3URL + "\" /data/ --region us-x-west-1 && chmod +x " + scriptName + " && ./" + scriptName
    pln("Running job script: " + s3URL + "...")
    val retVal = startShellProcess(cmd)
    pln("Done running job script.")
    return retVal
}

def getS3KeyFileName(key: String): String = {
    if (key.contains("/")) {
        return key.substring(key.lastIndexOf("/") + 1)
    }
    return ""
}

def getFileExtension(fileName: String): String = {
    if (fileName.contains(".")) {
        return fileName.substring(fileName.lastIndexOf(".") + 1)
    }
    return ""
}

def run(): ProcessResult = {
    var s3Dirs = ListBuffer[String]()

    s3Dirs = ListBuffer[String](
        "01-Extracted-ZSA-Data/1/",
        "01-Extracted-ZSA-Data/2/"
    )
    
    var zsaSchema = HashMap[String, ZSASchemaTableField]()
    zsaSchema = resetZSASchemaTableTypes(getZSASchemaFromS3)

    var processResult = new ProcessResult()

    processResult = process(
        s3Dirs = s3Dirs, paralellize = true, partitions = 500, s3KeyBatchSize = 1000000, aggregateCSV = true, writeJSON = true, writeCSV = true, writeSchema = true, 
        maxDocuments = Long.MaxValue, maxS3Keys = Long.MaxValue, maxDocumentsPerS3Key = Long.MaxValue, csvDocumentBatchSize = 1000, zsaSchemaTable = zsaSchema
        , keySpace = "zsa_raw_data", writeS3KeyPrefix = "", writeS3KeyPostfix = writeS3KeyPostfix, oneTypeAtATime = true, onlyAggregate = false, entityToAggregate = "Attachments"
    )


    if (processResult.loadScript.length > 0) {
        runS3JobScript(processResult.loadScript)    
    }

    return processResult
}

val processStart = LocalDateTime.now()
pln(sc.getExecutorMemoryStatus)

val processResult = run()

pln("")
pln(processResult)
pln("")

val processEnd = LocalDateTime.now()
val duration = Duration.between(processStart, processEnd)

pln("Process start:    " + processStart.toString())
pln("Process end:      " + processEnd.toString())
pln("Process run time: " + formatDuration(duration))
