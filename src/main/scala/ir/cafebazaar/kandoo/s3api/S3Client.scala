package ir.cafebazaar.kandoo.s3api

import java.io.{BufferedReader, File, InputStream, InputStreamReader}
import java.util
import java.util.zip.GZIPInputStream

import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.services.s3.{AmazonS3Client, S3ClientOptions}

/**
  * Created by alirabiee on 8/28/17.
  */
class S3Client(val bucket: String = "zeppelin-data") {
  private val clientOptions = new S3ClientOptions();
  clientOptions.setPathStyleAccess(true);

  private val s3Client =
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
      new InputStreamReader(
        inputStream, "UTF-8")
    )
  }
}
