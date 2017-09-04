package ir.cafebazaar.kandoo.s3api

import java.io.{BufferedReader, File, InputStream, InputStreamReader}
import java.util
import java.util.zip.GZIPInputStream

import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.{AmazonS3Client, S3ClientOptions}
import org.apache.zeppelin.spark.ZeppelinContext

/**
  * Created by alirabiee on 8/28/17.
  */
class S3Client(val bucket: String = "zeppelin-data", val userBucket: String = "zeppelin-user-data") {
  private val clientOptions = new S3ClientOptions();
  clientOptions.setPathStyleAccess(true);

  val s3Client =
    new AmazonS3Client(new PropertiesCredentials(new File("/etc/s3_credentials.conf")));

  s3Client.setEndpoint("http://kise.roo.cloud");
  s3Client.setS3ClientOptions(clientOptions);

  def ls(prefix: String = "", namePattern: String = ".*"): util.Set[String] = {
    val summaries = s3Client.listObjects(bucket, prefix).getObjectSummaries.iterator
    val result: util.Map[String, Boolean] = new util.HashMap

    while (summaries.hasNext) {
      val itemName: String = summaries.next.getKey.replaceFirst(prefix + "[/\\\\]?", "").split("[/\\\\]")(0)

      if (itemName.matches(namePattern)) {
        result.put(itemName, true)
      }
    }

    result.keySet()
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
