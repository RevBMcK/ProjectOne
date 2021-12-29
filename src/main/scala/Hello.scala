import org.apache.spark.sql.SparkSession
import sys.process._

object Hello {

  def main(args: Array[String]): Unit = {
    println("Hello")

    System.setProperty("hadoop.home.dir", "C:\\Hadoop")
    val spark = SparkSession
      .builder
      .appName("Hello Hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")

    /*def UserLogin (uN: String, uP: String) : String {

    }
    val username = "useradd TestName"
    val password = "passwd TestName"

    scala.io.StdIn.readLine(username)
    scala.io.StdIn.readLine(password)

    println(username)
    println(password)*/

    /*val showu = "ls -al" !

    println(showu)*/
    spark.sql("DROP TABLE POTDPartyData")
    spark.sql("CREATE TABLE IF NOT EXISTS POTDPartyData(ID INT, Rank INT, Name STRING, Server STRING, Datacenter STRING, Job STRING, Score INT, Floor INT, Date STRING) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'POTDParty.txt' INTO TABLE POTDPartyData")
    spark.sql("SELECT * FROM POTDPartyData").show()
    spark.sql("SELECT DISTINCT ID, Name, Score, Date FROM POTDPartyData WHERE Server = 'Exodus'").show()
    spark.sql("SELECT DISTINCT ID, Name, Job, Score, Date FROM POTDPartyData WHERE Job = 'Machinist'").show()

    /*spark.sql("CREATE TABLE IF NOT EXISTS POTDSoloData(ID INT, Rank INT, Name STRING, Server STRING, Datacenter STRING, Job STRING, Score INT, Floor INT, Date STRING) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'POTDParty.txt' INTO TABLE POTDPartyData")
    spark.sql("SELECT * FROM POTDPartyData").show()
    spark.sql("SELECT DISTINCT ID, Name, Score, Date FROM POTDPartyData WHERE Server = 'Exodus'").show()*/

    /*spark.sql("CREATE TABLE IF NOT EXISTS TestTable(id INT, name STRING) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'kv1.txt' INTO TABLE TestTable")
    spark.sql("SELECT * FROM TestTable").show()

    spark.sql("DROP TABLE IF EXISTS BevA")
    spark.sql("CREATE TABLE BevA(Beverage STRING, BranchID String) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'Bev_BranchA.txt' INTO TABLE BevA")
    spark.sql("SELECT * FROM BevA").show()

    spark.sql("DROP TABLE IF EXISTS BevB")
    spark.sql("CREATE TABLE BevB(Beverage STRING, BranchID String)row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'Bev_BranchB.txt' INTO TABLE BevB")
    spark.sql("SELECT * FROM BevB").show()

    spark.sql("SELECT * FROM TestTable").show()
    spark.sql("SELECT * FROM BevA").show()
    spark.sql("SELECT * FROM BevB").show()*/

    spark.close()

  }

}
