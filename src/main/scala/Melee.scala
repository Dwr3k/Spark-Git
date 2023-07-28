import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Melee{

  def main(args:Array[String]):Unit = {
    //val url = s"jdbc:sqlite:${args(0)}"
    val url = args(0)
    //val url = "jdbc:sqlite:C:\\Users\\Consultant\\Desktop\\melee_player_database.db"
    //  System.setProperty("hadoop.home.dir", "C:\\Users\\Consultant\\Documents\\hadoop-2.8.1\\hadoop-2.8.1")

    Logger.getLogger("org").setLevel(Level.ERROR)
    println(s"URL == $url")

    val sparkConf = new SparkConf()

    //  sparkConf.set("spark.master", "local")
    sparkConf.set("spark.app.name", "Melee Pipeline")
    sparkConf.set("spark.jars.packages", "org.xerial:sqlite-jdbc:3.36.0.3")

    val spark = SparkSession.builder().master("local").config(sparkConf).getOrCreate()

    val meleeSets = spark.read.format("jdbc").options(Map("driver" -> "org.sqlite.JDBC", "url" -> args(0), "dbtable" -> "sets")).load()
    val meleePlayers = spark.read.format("jdbc").options(Map("driver" -> "org.sqlite.JDBC", "url" -> url, "dbtable" -> "players")).load()



    //    val meleePlayers = spark.read.format("jdbc").options(Map("url" -> url, "dbtable" -> "players")).load()
//    val meleeSets = spark.read.format("jdbc").options(Map("url" -> url, "dbtable" -> "sets")).load()

    val rdd1 = meleePlayers.select("player_id", "state", "country").join(meleeSets.select("winner_id", "p1_id", "p2_id"), meleePlayers("player_id") === meleeSets("winner_id"), "inner")
    rdd1.show()
  }
}
