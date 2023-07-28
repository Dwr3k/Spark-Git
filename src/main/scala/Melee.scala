import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count

object Melee{

  def main(args:Array[String]):Unit = {
    val url = s"jdbc:postgresql://${args(0)}"
    //val outputPath = args(1)
//    val url = "jdbc:sqlite:C:\\Users\\Consultant\\Desktop\\melee_player_database.db"
    //  System.setProperty("hadoop.home.dir", "C:\\Users\\Consultant\\Documents\\hadoop-2.8.1\\hadoop-2.8.1")

    Logger.getLogger("org").setLevel(Level.ERROR)
    //println(s"URL == $url")

    val sparkConf = new SparkConf()

    //sparkConf.set("spark.master", "local")
    sparkConf.set("spark.app.name", "Horse Pipeline")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

//    val allDataDF = spark.read.option("header", true).option("delimiter", ";").csv(args(1))

    val allData = spark.read.format("jdbc").options(Map("driver" -> "org.postgresql.jdbc", "url" -> url, "dbtable" -> "drem_horses", "usermame" -> args(1), "password" -> args(2))).load()

    //select jockey, horseid, count(jockey) as num_races, round(avg("Final place")) average_finish from drem_horses group by jockey,horseid order by num_races desc, average_finish asc ;
    allData.show()



//    val meleeSets = spark.read.format("jdbc").options(Map("driver" -> "org.sqlite.JDBC", "url" -> url.trim(), "dbtable" -> "(select * from players)")).load()
//    val meleePlayers = spark.read.format("jdbc").options(Map("driver" -> "org.sqlite.JDBC", "url" -> url.trim(), "dbtable" -> "players")).load()



    //    val meleePlayers = spark.read.format("jdbc").options(Map("url" -> url, "dbtable" -> "players")).load()
//    val meleeSets = spark.read.format("jdbc").options(Map("url" -> url, "dbtable" -> "sets")).load()

//    val rdd1 = meleePlayers.select("player_id", "state", "country").join(meleeSets.select("winner_id", "p1_id", "p2_id"), meleePlayers("player_id") === meleeSets("winner_id"), "inner")
//    rdd1.show()
  }
}
