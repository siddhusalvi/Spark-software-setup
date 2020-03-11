import org.apache.spark.{SparkConf, SparkContext}

object s3wordcounter extends App {
  val myAccessKey = "AKIAJF2DZFGGEYETALOQ"
  val mySecretKey = "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"
  val bucket = "sparkweek6"
  val filepath = "words.csv"

  val conf = new SparkConf().setAppName("s3wordcounter").setMaster("local")
  val sc = new SparkContext(conf)
  val hadoopConf = sc.hadoopConfiguration
  hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  hadoopConf.set("fs.s3n.awsAccessKeyId",myAccessKey)
  hadoopConf.set("fs.s3n.awsSecretAccessKey",mySecretKey)

  val s3data = sc.textFile("s3n://"+bucket+"/"+filepath)
  val words = s3data.flatMap(line => line.split(" "))
  val pairs = words.map(word => (word,1))
  val wordcounts = pairs.reduceByKey(_+_)
  wordcounts.collect()


}
