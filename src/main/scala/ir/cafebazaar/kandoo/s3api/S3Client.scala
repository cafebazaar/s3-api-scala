package ir.cafebazaar.kandoo.s3api

import java.io.{BufferedReader, File, InputStream, InputStreamReader}
import java.util
import java.util.zip.GZIPInputStream

import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.services.s3.model.{GetObjectRequest, ListObjectsRequest, ObjectListing}
import com.amazonaws.services.s3.{AmazonS3Client, S3ClientOptions}
import org.apache.zeppelin.spark.ZeppelinContext

import collection.JavaConverters._

/**
  * Created by alirabiee on 8/28/17.
  */
class S3Client(val bucket: String = "zeppelin-data", val userBucket: String = "zeppelin-user-data") {
  private val clientOptions = new S3ClientOptions()
  clientOptions.setPathStyleAccess(true)

  private val credentialsFile: File = new File("/etc/s3_credentials.conf")

  private val s3Client = if (credentialsFile.exists()) new AmazonS3Client(new PropertiesCredentials(credentialsFile)) else null

  if (s3Client != null) {
    s3Client.setEndpoint("http://kise.roo.cloud")
    s3Client.setS3ClientOptions(clientOptions)
  }

  def this() {
    this(bucket = "zeppelin-data")
  }

  def ls(path: String = "", pattern: String = ".*", absolute: Boolean = false, limit: Integer = 64): List[String] = {
    val pfx = if(!path.equals("")) path.replaceFirst("^[/\\\\]?(.*?)[/\\\\]?$", "$1/") else ""
    val request = new ListObjectsRequest(bucket, pfx, null, "/", Integer.MAX_VALUE)
    val objectListing: ObjectListing = s3Client.listObjects(request)
    val prefixIterator = objectListing.getCommonPrefixes.iterator
    val result: util.Map[String, Boolean] = new util.HashMap

    while (prefixIterator.hasNext) {
      val itemName: String = prefixIterator.next.replaceFirst(pfx, "").split("/")(0)

      if (itemName.matches(pattern)) {
        if (absolute) {
          result.put(s"s3a://$bucket/$pfx$itemName", true)
        } else {
          result.put(itemName, true)
        }
      }
    }

    if(result.isEmpty) {
      var truncated = false
      do {
        val summaryIterator = objectListing.getObjectSummaries.iterator
        while (summaryIterator.hasNext && (limit <= 0 || result.size() < limit)) {
          val itemName: String = summaryIterator.next.getKey.replaceFirst(pfx, "").split("/")(0)

          if (itemName.matches(pattern)) {
            if (absolute) {
              result.put(s"s3a://$bucket/$pfx$itemName", true)
            } else {
              result.put(itemName, true)
            }
          }
        }
        truncated = objectListing.isTruncated && (limit <= 0 || result.size() < limit)
        if(truncated) {
          s3Client.listNextBatchOfObjects(objectListing)
        }
      } while (truncated)
    }

    result.keySet().asScala.toList
  }

  def readFile(filename: String): BufferedReader = {
    var inputStream: InputStream = null

    if (filename.toLowerCase.endsWith(".gz")) {
      inputStream = new GZIPInputStream(s3Client.getObject(bucket, filename.replaceFirst(s"//$bucket/?", "")).getObjectContent())
    } else {
      inputStream = s3Client.getObject(bucket, filename.replaceFirst(s"//$bucket/?", "")).getObjectContent()
    }

    new BufferedReader(
      new InputStreamReader(inputStream, "UTF-8")
    )
  }

  def makeUploadPath(filename: String, zeppelinContext: ZeppelinContext): String = {
    val user: String = zeppelinContext.getInterpreterContext.getAuthenticationInfo.getUser
    s"s3a://$userBucket/user/$user/$filename"
  }

  def path(filename: String, zeppelinContext: ZeppelinContext): String = {
    makeUploadPath(filename, zeppelinContext)
  }

  def downloadFile(filename: String, file: File = null): Unit = {
    var f = file

    if (file == null) {
      f = new File(filename.split("/").reverseIterator.next())
    }

    s3Client.getObject(new GetObjectRequest(bucket, filename), f)
  }
}
