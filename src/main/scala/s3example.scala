import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.GetObjectRequest
import scala.io.Source

object s3example extends App {

  val cred = new BasicAWSCredentials("AKIAJF2DZFGGEYETALOQ", "wSCnvoupSglH3P19qkiFVWSKrg+Ho/RcFxFrmRAG")
  val s3Client = new AmazonS3Client(cred)
  val s3Object = s3Client.getObject(new GetObjectRequest("sparkweek6", "words.csv"))
  val myData = Source.fromInputStream(s3Object.getObjectContent).getLines()

  var data = ""
  for (line <- myData) {
    data += line.replace(",", " ")
  }
  print(data)
}
