import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split, explode, col, concat_ws, udf, lit}

object Transpose {
  def generate_transpose(inputFile: String, outputDirectory: String, spark: SparkSession): Unit = {

    //create the initial data frame i.e read the csv file , don't use inferschema
    //as the schema won't be consistent. Moreover we would like to avoid read overhead of inferschema
    val inputDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", true)
      .load(inputFile)

    //get the columns and convert comun names to spark col objects
    val columns = inputDF.columns.map(c => col(c))

    /* Create a new dataframe with schema entity timestamp value attribute
     entity      timestamp      value                        attribute
    e1             t1         e1_v1,e1_v2,e1_v3....e1_vn   a1,a2,a3,.....an
    e2             t2         e2_v1,e2_v2,e2_v3....e2_vn   a1,a2,a3,.....an
    e3             t3         e3_v1,e3_v2,e3_v3....e3_vn   a1,a2,a3,.....an
    e4             t4         e4_v1,e4_v2,e4_v3....e4_vn   a1,a2,a3,.....an
    e5             t5         e5_v1,e5_v2,e5_v3....e5_vn   a1,a2,a3,.....an
    .......................................................................
    .......................................................................
    e3             tn         en_v1,en_v2,en_v3....en_vn   a1,a2,a3,.....an
    */
    val initDF = inputDF.select(
      columns.head, columns.last,
      concat_ws(",", columns.slice(1, columns.size - 1): _*).as("value"))
      .withColumn(
        "attribute",
        lit(columns.slice(1, columns.size - 1).map(c => c.toString()).mkString(","))
      )

    val mergeav = udf((a: String, v: String) => {
      a.split(",") zip v.split(",") map (av => av._1 + "," + av._2) mkString ("-")
    })

    /*	create a new dataframe witj schema entity timestamp av
       entity      timestamp      av
        e1             t1         a1,e1_v1-a2,e1_v2-a3,e1_v3....-an,e1_vn
        e2             t2         a1,e2_v1-a2,e2_v2-a3,e2_v3....-an,e2_vn
        e3             t3         a1,e3_v1-a2,e3_v2-a3,e3_v3....-an,e3_vn
        e4             t4         a1,e4_v1-a2,e4_v2-a3,e4_v3....-an,e4_vn
        e5             t5         a1,e5_v1-a2,e5_v2-a3,e5_v3....-an,e5_vn
        e6             t6         a1,e6_v1-a2,e6_v2-a3,e6_v3....-an,e6_vn
        .......................................................................
        .......................................................................
        en             tn         a1,en_v1-a2,en_v2-a3,en_v3....-an,en_vn
*/
    val mergedDF = initDF.select(columns.head,
      columns.last,
      mergeav(col("attribute"), col("value")).as("av")
    )

    /* create a new dataframe witj schema entity atribute value timestamp
   first use explode convert each row in to multiple row i.e using - as splitter

   i.e e1             t1         a1,e1_v1-a2,e1_v2-a3,e1_v3....-an,e1_vn transfroms to
   e1  t1  a1,e1_a1
   e1  t1  a2,e1_a2
   ...
   e1  t1  an,e1_an
   then split column av in to column attribute & value
   The final schema should be as follows
   ClientID	Attribute	Value		timestamp
       e1   		a1      e1_v1		t1
       e1   		a2      e1_v2		t1
       e1   		a3      e1_v3		t1
       .............................
       e1   		an      e1_vn		t2
       e2   		a1      e2_v1		t2
       e2   		a2      e2_v2		t2
       e2   		a3      e2_v3		t2
       .............................
       e2   		an      e2_vn		t2
       .............................
       .............................
       en   		an-1    en_vn-1	tn
       en   		an      en_vn		tn
*/
    val finalDF = mergedDF.select(columns.head,
      columns.last, explode(split(col("av"), "-")).alias("av"))
      .select(columns.head,
        split(col("av"), ",").getItem(0).as("attribute"),
        split(col("av"), ",").getItem(1).as("value"),
        columns.last
      )

    finalDF.write.csv(outputDirectory)
  }

  def main(args: Array[String]): Unit = {
    //Create the spark session
    val spark = SparkSession.builder.appName("Transpose").getOrCreate()
    generate_transpose(args(0), args(1), spark)
    spark.stop()
  }
}
