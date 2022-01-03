import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHive.sparkContext

import sys.process._

object Hello {

  def main(args: Array[String]): Unit = {
    var userAdminData = ""

    println("Hello")
    print("Input username: ")
    val userName = scala.io.StdIn.readLine()

    print("Input password: ")
    val password = scala.io.StdIn.readLine()

    if (userName == "Basic" && password == "123") {
      println("Welcome " + userName) }
    else if (userName == "Admin" && password == "456") {
      println("Welcome " + userName + ". You have partitioning and bucket permissions.") }
    else {
      println("Invalid user. Goodbye")
      System.exit(1) }

    System.setProperty("hadoop.home.dir", "C:\\Hadoop")
    val spark = SparkSession
      .builder
      .appName("Hello Hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")

    spark.sparkContext.setLogLevel("WARN")
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    //Works well
    spark.sql("DROP TABLE POTDPartyData")
    spark.sql("CREATE TABLE IF NOT EXISTS POTDPartyData(ID INT, Rank INT, Name STRING, Server STRING, Datacenter STRING, Job STRING, Score INT, Floor INT, Date STRING) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'POTDParty.txt' INTO TABLE POTDPartyData")
    /*spark.sql("SELECT * FROM POTDPartyData").show()
    spark.sql("SELECT DISTINCT ID, Name, Score, Date FROM POTDPartyData WHERE Server = 'Exodus'").show()
    spark.sql("SELECT DISTINCT ID, Name, Job, Score, Date FROM POTDPartyData WHERE Job = 'Machinist'").show()*/

    //Questions

    userI()

    def userI():Any = {
      println("Choose data to display (1-6) Type 'q' to quit")
      val userInput = scala.io.StdIn.readLine()

      userInput match {
        case "1" => FirstQ()
        case "2" => SecondQ()
        case "3" => ThirdQ()
        case "4" => FourthQ()
        case "5" => FiveQ()
        case "6" => SixQ()
        case "q" => System.exit(1)
      }
    }

    // Job with the Highest Overall Score
    def FirstQ():Any = {
      spark.sql("SELECT Job, Score FROM POTDPartyData ORDER BY Score DESC LIMIT 1").show()
      if (userName == "Admin")
        {
          println("Type 'p' to Partition Data | 'b' to Bucket Data | 'q' to quit")
          val userAdminInput = scala.io.StdIn.readLine()
          userAdminInput match {
            case "p" => print("Type name of new partition: ")
                        val userAdminPName = scala.io.StdIn.readLine()

                        print("Which parameters would you like to partition: ")
                        val userAdminP = scala.io.StdIn.readLine()
                        userAdminP match {
                          case "ID" => userAdminData = "ID INT"
                          case "Name" => userAdminData = "Name STRING"
                          case "Job" => userAdminData = "Date STRING"
                          case "Score" => userAdminData = "Score INT"
                          case "Floor" => userAdminData = "Floor INT"
                          case _ => println("Invalid Input.")
                                    userI()
                        }

                        spark.sql(s"DROP TABLE IF EXISTS $userAdminPName")
                        spark.sql(s"CREATE TABLE IF NOT EXISTS admin_temp (ID INT, Rank INT, Name STRING) row format delimited fields terminated by ','")
                        spark.sql(s"LOAD DATA LOCAL INPATH 'POTDParty.txt' INTO TABLE admin_temp")
                        spark.sql(s"CREATE TABLE $userAdminPName (ID INT) PARTITIONED BY (Name STRING) row format delimited fields terminated by ','")
                        spark.sql(s"INSERT INTO $userAdminPName (ID) SELECT ID, Rank, Name FROM admin_temp")
                        println("Your new partition: ")
                        spark.sql((s"SHOW PARTITIONS $userAdminPName")).show()

            case "q" => userI()
          }
        }
      else {userI()}
    }

    // Job that completes POTD the most
    def SecondQ():Any = {
      spark.sql("SELECT Job, Floor, COUNT(*) as Total FROM POTDPartyData GROUP BY Job, Floor ORDER BY Total DESC LIMIT 1").show()
      userI()
    }

    // Most played Job
    def ThirdQ():Any = {
      spark.sql("SELECT Job, count(*) as Total FROM POTDPartyData GROUP BY Job ORDER BY Total DESC LIMIT 1").show()
      userI()
    }

    // Player with most recorded scores
    def FourthQ():Any= {
      spark.sql("SELECT Name,count(*) as Total FROM POTDPartyData GROUP BY Name ORDER BY Total DESC LIMIT 1").show()
      userI()
    }

    // Date with most scores recorded
    def FiveQ():Any= {
      spark.sql("SELECT Name, Job, Score, Date, COUNT(*) as Total FROM POTDPartyData GROUP BY Name, Job, Score, Date ORDER BY Total DESC").show()
      userI()
    }

    // Top Servers with the most clears
    def SixQ():Any = {
      spark.sql("SELECT Server,count(*) as Total FROM POTDPartyData GROUP BY Server ORDER BY Total DESC LIMIT 2").show()
      userI()
    }


    //Reading my csv file
    /*var df3 = spark.read.options(Map("header" -> "true"))
      .csv("POTDSoloPal.csv")
    df3.show()

    df3.select("Name").show()*/

    /*spark.sql("CREATE TABLE IF NOT EXISTS POTDSoloTest(ID INT, Rank INT, Name STRING, Data_Center STRING, Job STRING, Score INT, Floor STRING, Date STRING) row format delimited fields terminated by ',' enclosed by ''' lines terminated by'/n'")
    spark.sql("LOAD DATA LOCAL INPATH 'POTDSoloPal.csv' INTO TABLE POTDSoloTest") //FIELDS TERMINATED BY ',' ENCLOSED BY ''' LINES TERMINATED BY '/n' IGNORE 1 ROWS")
    spark.sql("SELECT * FROM POTDSoloPal").show()*/

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
