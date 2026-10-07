import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object Day11App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 11 - Broadcast and Accumulators")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    val sc = spark.sparkContext

    println("===== DAY 11: BROADCAST AND ACCUMULATORS =====")

    val productMaster = Map(
      "P101" -> "Laptop",
      "P102" -> "Mobile",
      "P103" -> "Headphones",
      "P104" -> "Keyboard",
      "P105" -> "Monitor"
    )

    val broadcastProducts = sc.broadcast(productMaster)

    val badRecords: LongAccumulator =
      sc.longAccumulator("Bad Records")

    val transactions = Seq(
      ("T001", "P101", 70000.0),
      ("T002", "P102", 30000.0),
      ("T003", "P999", 10000.0),
      ("T004", "P103", 5000.0),
      ("T005", "P888", 15000.0),
      ("T006", "P104", 3000.0)
    )

    val transactionRDD = sc.parallelize(transactions)

    val validated = transactionRDD.map {
      case (transactionId, productId, amount) =>

        broadcastProducts.value.get(productId) match {

          case Some(productName) =>
            (transactionId, productId, productName, amount, "VALID")

          case None =>
            badRecords.add(1)
            (transactionId, productId, "UNKNOWN", amount, "INVALID")
        }
    }

    println("\nALL TRANSACTIONS:")
    validated.collect().foreach(println)

    println("\nVALID TRANSACTIONS:")
    validated
      .filter(_._5 == "VALID")
      .collect()
      .foreach(println)

    println("\nINVALID TRANSACTIONS:")
    validated
      .filter(_._5 == "INVALID")
      .collect()
      .foreach(println)

    println("\nBAD RECORD COUNT:")
    println(badRecords.value)

    println("\nWHY BROADCAST?")
    println("A small read-only master table can be broadcast to executors.")
    println("This avoids repeatedly sending the same reference data with tasks.")

    println("\nWHY ACCUMULATOR?")
    println("Accumulators allow distributed tasks to update counters safely.")

    println("\nWHY NOT NORMAL DRIVER VARIABLES?")
    println("Normal driver variables are not designed for distributed updates.")
    println("Executors run tasks independently and their local changes are not")
    println("reliably reflected back in the driver variable.")

    spark.stop()
  }
}
