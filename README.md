# Data Engineering with Scala and Spark

<a href="https://www.packtpub.com/product/data-engineering-with-scala-and-spark/9781804612583"><img src="https://content.packt.com/B18992/cover_image_small.jpg" alt="no-image" height="256px" align="right"></a>

This is the code repository for [Data Engineering with Scala and Spark](https://www.packtpub.com/product/data-engineering-with-scala-and-spark/9781804612583), published by Packt.

**Build streaming and batch pipelines that process massive amounts of data using Scala**

## What is this book about?
Learn new techniques to ingest, transform, merge, and deliver trusted data to downstream users using modern cloud data architectures and Scala, and learn end-to-end data engineering that will make you the most valuable asset on your data team.

This book covers the following exciting features:
* Set up your development environment to build pipelines in Scala
* Get to grips with polymorphic functions, type parameterization, and Scala implicits
* Use Spark DataFrames, Datasets, and Spark SQL with Scala
* Read and write data to object stores
* Profile and clean your data using Deequ
* Performance tune your data pipelines using Scala

If you feel this book is for you, get your [copy](https://www.amazon.com/Data-Engineering-Scala-Spark-streaming/dp/1804612588/ref=sr_1_1?crid=2J2YYMLA50V7J&keywords=Data+Engineering+with+Scala+and+Spark&qid=1707115672&sprefix=data+engineering+with+scala+and+spark%2Caps%2C399&sr=8-1) today!
<a href="https://www.packtpub.com/?utm_source=github&utm_medium=banner&utm_campaign=GitHubBanner"><img src="https://raw.githubusercontent.com/PacktPublishing/GitHub/master/GitHub.png" 
alt="https://www.packtpub.com/" border="5" /></a>
## Instructions and Navigations
All of the code is organized into folders. For example,

The code will look like the following:
```
val updateSilver: DataFrame = bronzeData
   .select(from_json(col("value"), jsonSchema).alias("value"))
   .select(
     col("value.device_id"),
     col("value.country"),
     col("value.event_type"),
     col("value.event_ts")
    )
   .dropDuplicates("device_id", "country", "event_ts")
```

**Following is what you need for this book:**
This book is for data engineers who have experience in working with data and want to understand how to transform raw data into a clean, trusted, and valuable source of information for their organization using Scala and the latest cloud technologies.

With the following software and hardware list you can run all code files present in the book (Chapter 1-13).
## Software and Hardware List
| Chapter | Software required | OS required |
| -------- | ------------------------------------ | ----------------------------------- |
| 1-13 | Microsoft Azure | Windows, macOS, or Linux  |
| 1-13 | Databricks Community Edition | Windows, macOS, or Linux  |
| 1-13 | JDK 8 |  Windows, macOS, or Linux  |
| 1-13 | Intellij IDEA | Windows, macOS, or Linux  |
| 1-13 | VS Code |  Windows, macOS, or Linux  |
| 1-13 | Docker Community Edition |  Windows, macOS, or Linux  |
| 1-13 | Apache Spark 3.3.1 | Windows, macOS, or Linux  |
| 1-13 | MySql |  Windows, macOS, or Linux  |
| 1-13 | MinIO | Windows, macOS, or Linux  |


## Related products
* Cracking the Data Engineering Interview [[Packt]](https://www.packtpub.com/product/cracking-the-data-engineering-interview/9781837630776) [[Amazon]](https://www.amazon.com/Cracking-Data-Engineering-Interview-resume-building/dp/1837630771/ref=sr_1_1?crid=3PUUC05LSAGDU&keywords=Cracking+the+Data+Engineering+Interview&qid=1707116597&sprefix=cracking+the+data+engineering+interview%2Caps%2C382&sr=8-1)

* Data Engineering with dbt [[Packt]](https://www.packtpub.com/product/data-engineering-with-dbt/9781803246284) [[Amazon]](https://www.amazon.com/Data-Engineering-dbt-cloud-based-dependable/dp/1803246286/ref=sr_1_1?crid=27GG4L8IXASOS&keywords=Data+Engineering+with+dbt&qid=1707116670&sprefix=data+engineering+with+dbt%2Caps%2C394&sr=8-1)

## Get to Know the Authors
**Eric Tome**
 has over 25 years of experience working with data. He has contributed to and led teams that ingested, cleansed, standardized, and prepared data used by business intelligence, data science, and operations teams. He has a background in Mathematics and currently works as a Solutions Architect for Databricks, helping customers solve their data

**David Radford**
 has worked in big data for over ten years with a focus on cloud technologies. He led consulting teams for multiple years completing migrations from legacy systems to modern data stacks. He holds a Master's degree in Computer Science and works as a Solutions Architect at Databricks.and AI challenges.

**Rupam Bhattacharjee**
 works as a Lead Data Engineer at IBM. He has architected and developed data pipelines processing massive structured and unstructured data using Spark and Scala for on-prem Hadoop and k8s clusters on the public cloud. He has a degree in Electrical Engineering.


