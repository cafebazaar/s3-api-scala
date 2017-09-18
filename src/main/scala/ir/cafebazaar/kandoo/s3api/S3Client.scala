package ir.cafebazaar.kandoo.s3api

import java.io.{BufferedReader, File, InputStream, InputStreamReader}
import java.util
import java.util.zip.GZIPInputStream

import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.{AmazonS3Client, S3ClientOptions}
import org.apache.zeppelin.spark.ZeppelinContext

import scala.collection.JavaConversions._

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

  def ls(prefix: String = "", namePattern: String = ".*", fullPath: Boolean = false): List[String] = {
    val summaries = s3Client.listObjects(bucket, prefix).getObjectSummaries.iterator
    val result: util.Map[String, Boolean] = new util.HashMap

    val pfx = prefix.replaceFirst("^[/\\\\]?(.*?)[/\\\\]?$", "$1")

    while (summaries.hasNext) {
      val itemName: String = summaries.next.getKey.replaceFirst(pfx + "[/\\\\]?", "").split("[/\\\\]")(0)

      if (itemName.matches(namePattern)) {
        if(fullPath) {
          result.put(s"//$bucket/$pfx/$itemName", true)
        } else {
          result.put(itemName, true)
        }
      }
    }

    result.keySet().toList
  }

  def readFile(filename: String): BufferedReader = {
    var inputStream: InputStream = null

    if (filename.toLowerCase.endsWith(".gz")) {
      inputStream = new GZIPInputStream(s3Client.getObject(bucket, filename).getObjectContent())
    } else {
      inputStream = s3Client.getObject(bucket, filename).getObjectContent()
    }

    new BufferedReader(
      new InputStreamReader(inputStream, "UTF-8")
    )
  }

  def makeUploadPath(filename: String, zeppelinContext: ZeppelinContext): String = {
    val user: String = zeppelinContext.getInterpreterContext.getAuthenticationInfo.getUser
    s"s3a://$userBucket/user/$user/$filename"
  }

  def downloadFile(filename: String, file: File = null): Unit = {
    var f = file

    if (file == null) {
      f = new File(filename.split("/").reverseIterator.next())
    }

    s3Client.getObject(new GetObjectRequest(bucket, filename), f)
  }
}
