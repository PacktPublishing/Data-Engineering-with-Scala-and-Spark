package com.packt.dewithscala.chapter5

import com.packt.dewithscala.Config

import com.amazonaws.ClientConfiguration
import com.amazonaws.Protocol
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.cloudfront.model.EndPoint
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.util.SdkHttpUtils

import scala.io.Source
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object WorkingWithObjectStores extends App {

  private val bucket      = "scala-data-engineering"
  private val objectPath  = "my/first/object.csv"
  private val endPoint    = Config.cfg.objectstore.head.endpoint
  private val accessKey   = Config.cfg.objectstore.head.accesskey.value
  private val secretKey   = Config.cfg.objectstore.head.secretkey.value
  private val credentials = new BasicAWSCredentials(accessKey, secretKey)

  // build a s3 client to work with minio installed in chapter 2
  private val s3Client = AmazonS3ClientBuilder
    .standard()
    .withCredentials(new AWSStaticCredentialsProvider(credentials))
    .withPathStyleAccessEnabled(true)
    .withClientConfiguration(
      new ClientConfiguration().withProtocol(Protocol.HTTP)
    )
    .withEndpointConfiguration(
      new AwsClientBuilder.EndpointConfiguration(
        endPoint,
        Regions.US_EAST_1.name()
      )
    )
    .build()

  // upload an object
  s3Client.putObject(bucket, objectPath, "id,name\n1,john")

  private val s3Object =
    s3Client.getObject(new GetObjectRequest(bucket, objectPath))

  // read an object
  private val myData =
    Source.fromInputStream(s3Object.getObjectContent()).getLines()

  // print content
  for (line <- myData) println(line)

  val session = SparkSession
    .builder()
    .appName("Read and write data to minio")
    .config("fs.s3a.endpoint", endPoint)
    .config("fs.s3a.access.key", accessKey)
    .config("fs.s3a.secret.key", secretKey)
    .config("fs.s3a.connection.ssl.enabled", "false")
    .config("fs.s3a.path.style.access", "true")
    .master("local[*]")
    .getOrCreate()

  // read data from minio
  val df = session.read
    .option("header", "true")
    .csv(s"s3a://$bucket/$objectPath")

  df.show()

  // write to minio
  val writePath = "my/first-write"
  df.write
    .mode(SaveMode.Overwrite)
    .csv(s"s3a://$bucket/$writePath")

  println("********************list objects********************")
  s3Client
    .listObjects(
      new ListObjectsRequest()
        .withBucketName(bucket)
        .withPrefix(writePath)
    )
    .getObjectSummaries()
    .asScala
    .toList
    .map(_.getKey())
    .foreach(println)
  println("****************************************************")

//  delete objects
  s3Client.deleteObject(bucket, objectPath)
  s3Client.deleteObject(bucket, writePath)

}
