import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count
object Melee{

  def main(args:Array[String]):Unit = {
    val url = s"jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com/testdb"
//    val url = "jdbc:sqlite:C:\\Users\\Consultant\\Desktop\\DemoDB\\melee_player_database.db"
//    System.setProperty("hadoop.home.dir", "C:\\Users\\Consultant\\Documents\\hadoop-2.8.1\\hadoop-2.8.1")

    Logger.getLogger("org").setLevel(Level.ERROR)
    println(s"URL == $url")

    val sparkConf = new SparkConf()

    sparkConf.set("spark.master", "local")
    sparkConf.set("spark.app.name", "Melee Pipeline")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val username: String = args(0)
    val password: String = args(1)

    val players = spark.read.format("jdbc").options(Map("driver" -> "org.postgresql.Driver", "fetchsize" -> "10000","url" -> url.trim(), "dbtable" -> "players", "user" -> username, "password" -> password)).load()
    val sets = spark.read.format("jdbc").options(Map("driver" -> "org.postgresql.Driver", "fetchsize" -> "10000", "url" -> url.trim(), "dbtable" -> "sets", "user" -> username, "password" -> password)).load()
    val tournamentInfo = spark.read.format("jdbc").options(Map("driver" -> "org.postgresql.Driver","fetchsize" -> "10000", "url" -> url.trim(), "dbtable" -> "tournament_info", "user" -> username, "password" -> password)).load()

    players.show()
    sets.show()
    tournamentInfo.show()


//    players.createTempView("players")
//    sets.createTempView("sets")
//    tournamentInfo.createTempView("tournament_info")
//
//    val gf1 = spark.sql("Select p1.tag, p2.tag, count(*) as times_won " +
//      "from sets s join players w on s.winner_id = w.player_id " +
//      "join players p1 on p1.player_id = s.p1_id " +
//      "join players p2 on p2.player_id = s.p2_id " +
//      "where (p1.tag = \"Leffen\" and p2.tag = \"Armada\") or (p1.tag = \"Armada\" and p2.tag = \"Leffen\") \ngroup by p1.tag, p2.tag\norder by times_won DESC")
//    //gf1.repartition(1).write.mode("overwrite").csv(args(2))
//    gf1.show()
//
//
//    val gf2 = spark.sql("select w.tag as Winner, p1.tag as Player_1, p2.tag as Player_2, p1_score, p2_score, tournament_key from sets join players w on winner_id = w.player_id " +
//      "join players p1 on p1_id = p1.player_id\njoin players p2 on p2_id = p2.player_id\njoin tournament_info ti on ti.key = sets.tournament_key " +
//      "where sets.location_names = '[\"GFR\", \"GF Reset\", \"Grand Final Reset\"]'  or sets.location_names = '[\"GF\", \"Grand Final\", \"Grand Final\"]'\norder by ti.start desc")
//    //gf2.repartition(1).write.mode("overwrite").csv()
//    gf2.show()
//
//
//    val gf3 = spark.sql("select state, count(state) as tourneys_held from tournament_info where online == 0 group by state order by tourneys_held desc")
//    gf3.show()










////    val allDataDF = spark.read.option("header", true).option("delimiter", ";").csv(args(1))
//
//    val allData = spark.read.format("jdbc").options(Map("driver" -> "org.postgresql.jdbc", "url" -> url, "dbtable" -> "drem_horses", "usermame" -> args(1), "password" -> args(2))).load()
//
//    //select jockey, horseid, count(jockey) as num_races, round(avg("Final place")) average_finish from drem_horses group by jockey,horseid order by num_races desc, average_finish asc ;
//    allData.show()







    //    val meleePlayers = spark.read.format("jdbc").options(Map("url" -> url, "dbtable" -> "players")).load()
//    val meleeSets = spark.read.format("jdbc").options(Map("url" -> url, "dbtable" -> "sets")).load()

//    val rdd1 = meleePlayers.select("player_id", "state", "country").join(meleeSets.select("winner_id", "p1_id", "p2_id"), meleePlayers("player_id") === meleeSets("winner_id"), "inner")
//    rdd1.show()
  }
}
