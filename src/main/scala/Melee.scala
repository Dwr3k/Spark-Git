import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Melee{

  def main(args:Array[String]):Unit = {
    val url = args(0)
    //  System.setProperty("hadoop.home.dir", "C:\\Users\\Consultant\\Documents\\hadoop-2.8.1\\hadoop-2.8.1")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf()
    //  sparkConf.set("spark.master", "local")
    sparkConf.set("spark.app.name", "Melee Pipeline")


    val spark = SparkSession.builder().master("local").config(sparkConf).getOrCreate()

    val meleePlayers = spark.read.format("jdbc").options(Map("url" -> url, "dbtable" -> "players")).load()
    val meleeSets = spark.read.format("jdbc").options(Map("url" -> url, "dbtable" -> "sets")).load()

    val rdd1 = meleePlayers.select("player_id", "state", "country").join(meleeSets.select("winner_id", "p1_id", "p2_id"), meleePlayers("player_id") === meleeSets("winner_id"), "inner")
    rdd1.show()
  }
}
