import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class Addresses(AddressCity: String,
                     AddressLine1: String,
                     AddressLine2: String,
                     AddressState: String,
                     AddressZIPCode: String,
                     RecordNumber: Int,
                     FirstName: String,
                     LastName: String,
                     Email: String,
                     PersonIndex: Int,
                     Gender: String,
                     HomePhone: String,
                     MaritalStatus: String,
                     PersonID: Int,
                     SSN: Int)

object SparkSQLRunner {
  def main(args: Array[String]): Unit = {
    // setup SparkSession instance

    val spark: SparkSession = SparkSession
      .builder()
      .appName("SparkSQL For Csv")
      .master("local[*]")
      .getOrCreate()

    // Read csv file
//    val df: DataFrame = spark.read.option("header", "true")
//      .option("delimiter", ",")
//      .csv("/Users/paul/ONS/spark-test/Person.csv")

    import spark.implicits._
    val ds: Dataset[Addresses] = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/Users/paul/ONS/spark-test/Person.csv")
      .as[Addresses]

    //ds.printSchema
    //ds.createOrReplaceTempView("person")

    val v: Dataset[(Int, String, Int, String)] =
      ds.select(
        'PersonId.as("PersonKey").as[Int],
        lit("XYZ").as("IdentifierName").as[String],
        'PersonIndex.as("Extension").as[Int],
        lit("A").as("Status").as[String]
      ).where('PersonId.isNotNull && 'PersonIndex.isNotNull)

    val w: Dataset[(Int, String, Int, String)] =
      ds.select(
        'PersonId.as("PersonKey").as[Int],
        lit("ABC").as("IdentifierName").as[String],
        'RecordNumber.as("Extension").as[Int],
        lit("A").as("RecordStatus").as[String]
      ).where('PersonId.isNotNull && 'RecordNumber.isNotNull)

    val x: Dataset[(Int, String, Int, String)] =
      ds.select(
        'PersonId.as("PersonKey").as[Int],
        lit("MNO").as("IdentifierName").as[String],
        'SSN.as("Extension").as[Int],
        lit("A").as("RecordStatus").as[String]
      ).where('PersonId.isNotNull && 'SSN.isNotNull)

    v.union(w)
      .union(x)
      .sort('PersonKey, 'IdentifierName, 'Extension)
      .show(50)

    // Optional. Calling printSchema prints the inferred schema. See output below.
    //df.printSchema()

    // Registers the DataFrame in form of view
    //df.createOrReplaceTempView("person")

    // Actual SparkSQL query
//    val sqlPersonDF = spark.sql(
//      """
//        |SELECT
//        |              PersonID AS PersonKey,
//        |                'XYZ' AS IdentifierName,
//        |                PersonIndex AS Extension,
//        |                'A' AS Status
//        |              FROM person
//        |              WHERE
//        |                PersonID IS NOT NULL AND PersonIndex IS NOT NULL
//        |              UNION
//        |              SELECT
//        |                PersonID AS PersonKey,
//        |                'ABC' AS IdentifierName,
//        |                RecordNumber AS Extension,
//        |                'A' AS RecordStatus
//        |              FROM person
//        |              WHERE
//        |                PersonID IS NOT NULL AND RecordNumber IS NOT NULL
//        |              UNION
//        |              SELECT
//        |                PersonID AS PersonKey,
//        |                'MNO' AS IdentifierName,
//        |                SSN AS Extension,
//        |                'A' AS RecordStatus
//        |              FROM person
//        |              WHERE
//        |                PersonID IS NOT NULL AND SSN IS NOT NULL
//      """.stripMargin)
//
//    // Print the result. See output below
//    sqlPersonDF.sort("PersonKey", "IdentifierName", "Extension")
//      .show(50)
  }

}
