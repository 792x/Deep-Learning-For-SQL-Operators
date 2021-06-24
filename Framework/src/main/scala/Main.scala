import java.io.File
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, ReturnAnswer, Sort, SubqueryAlias}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

import java.io.{BufferedWriter, FileWriter}
import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases

object Main {
    val sparkSession: SparkSession = SparkSession.builder
        .appName("Taster")
        .master("local[*]")
        .getOrCreate();

    val ROOT_LOGGER: Logger = Logger.getRootLogger
    ROOT_LOGGER.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    val DATA_SET: String = "phonelabs" // "skyserver" | "phonelabs" | "iot"
    val DATA_DIR: String = "/Users/falker/Documents/Projects/Deep-Learning-For-SQL-Operators/Framework/data/";
    val LOG_DIR: String = DATA_DIR + DATA_SET + "/logs/";
    val OUTPUT_DIR: String = DATA_DIR + DATA_SET + "/output/";
    val SCHEMA_DIR: String = DATA_DIR + DATA_SET + "/schema/";
    val USE_DATA_SUBSET: Boolean = false;
    val TOP_K = 5000;

    val tableCounter: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
    var subTreeCount: mutable.HashMap[LogicalPlan, Long] = new mutable.HashMap[LogicalPlan, Long]()
    var subTreeIds: mutable.HashMap[LogicalPlan, Long] = new mutable.HashMap[LogicalPlan, Long]()
    var dataframeSizes: Map[String, Int] = Map[String, Int]()






    def main(args: Array[String]): Unit = {
        val dataframes: Map[String, DataFrame] = loadSchema()
        val queries: List[String] = DATA_SET match {
            case "skyserver" => queryWorkloadSkyserver()
            case "phonelabs" => queryWorkloadPhoneLabs()
            case "iot" => queryWorkloadIoT()
        }

        println("Number of queries: " + queries.size)
        println(queries.head)
        println(queries(1))
        println(queries(2))
        println(queries.last)

        val processedQueries: Seq[Set[LogicalPlan]] = processQueries(queries)

        createTreeIds()

        val encodedQueries: Seq[Set[Long]] = encodeQueries(processedQueries)

        writeToCSV(encodedQueries)

        println("============FINISHED STATS============")
        println("Number of queries: " + queries.size)
        println("Total unique partial QEPs: " + subTreeCount.size)
        println("Top 10 frequent partial QEPs:")
        println(subTreeCount.toSeq.sortWith(_._2 > _._2).take(10))
        println("Frequency distribution of top " + TOP_K +" partial QEPs: " + subTreeCount.values.toList.sorted(Ordering.Long.reverse).take(TOP_K))
        println("Number of partial QEPs only occurring once: " + subTreeCount.values.toList.count(_ == 1))

        println("Processed queries: " + processedQueries.length + " out of " + queries.length)
        println("Finished encoding queries: " + processedQueries.length + " out of " + encodedQueries.length)



    }

    /**
     * Creates a hashmap of logical plans (partial QEP) and a unique id
     *
     * @param top_k the top_k frequent plans that are considered
     */
    def createTreeIds(top_k: Int = TOP_K): Unit = {
        val subTrees: Seq[(LogicalPlan, Long)] = subTreeCount.toSeq.sortWith(_._2 > _._2).take(top_k)
        for (i <- subTrees.indices) {
            subTreeIds(subTrees(i)._1) = i + 1
        }
    }


    /**
     * Loads the schema of the database by reading 10 rows of each table into its own dataframe
     *
     * @return a Map of dataframes accessible by table name
     */
    def loadSchema(): Map[String, DataFrame] = {
        val files: Array[File] = (new File(SCHEMA_DIR)).listFiles
        var dataframes: Map[String, DataFrame] = Map[String, DataFrame]()
        val indices: Int = if (USE_DATA_SUBSET) {
            println("Warning: using data subset of 10 tables")
            10
        } else {
            files.length
        }

        val delimiter: String = DATA_SET match {
            case "skyserver" => ","
            case "phonelabs" => ";"
            case "iot" => ","
        }

        for (i <- 0 until indices) {
            if (files(i).getName.endsWith(".csv")) {
                val filename: String = files(i).getName.substring(0, files(i).getName.lastIndexOf('.')).capitalize
                println("Reading: " + files(i).getName)
                try {
                    val loaded: DataFrame = sparkSession.sqlContext
                        .read.format("com.databricks.spark.csv")
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .option("delimiter", delimiter)
                        .option("nullValue", "null")
                        .load(SCHEMA_DIR + "/" + files(i).getName)
                    dataframes += (filename -> loaded)
                    sparkSession.sqlContext.createDataFrame(loaded.rdd, loaded.schema).createOrReplaceTempView(filename)
                } catch {
                    case e: Exception =>
                        println("Error occurred for: " + files(i).getName)
                        println(e)
                }
            }
        }
        sparkSession.catalog.listTables()
        dataframes.keys.foreach((k) => {
            sparkSession.table(k).printSchema()
            println(k, sparkSession.table(k).count())
        })
        dataframeSizes = dataframes.map(pair => (pair._1, pair._2.columns.length))
        dataframes
    }

    /**
     * Loads the query log from the phonelabs files into a list of usable SQL queries
     *
     * @return a List of query SQL statements
     */
    def queryWorkloadPhoneLabs(): List[String] = {
        var queries: ListBuffer[String] = ListBuffer();

        var src: Iterator[String] = Source.fromFile(LOG_DIR + "2747d54967a32fc95945671b930d57c1d5a9ac02_log.csv").getLines
        src.take(1).next
        for (l <- src) {
//            println(l.split(";")(3)) // can cause array out of bounds exception for some queries
            // Filter for first day and first hour only to limit number of queries
            if ((l.contains("2015-03-01") ||l.contains("2015-03-02") || l.contains("2015-03-03") || l.contains("2015-03-04") || l.contains("2015-03-05")) && !l.contains("sqlite_master") && !l.contains("INSERT") && l.contains("SELECT") && !l.contains("DELETE") && !l.contains("REPLACE") && !l.contains("UPSERT") && !l.contains("UPDATE")) {
                // Replace question marks with 0
                var sql_query: String = l.split(";")(3).replace("?", "0").replace("()", "(0)").replace("?group", "? group");
                // Remove column specifiers for INSERT INTO queries and add 0 for all columns
                if (sql_query.contains("INSERT") && sql_query.contains("INTO") && sql_query.contains("VALUES")) {
                    sql_query = sql_query.replaceFirst("\\(([^\\)]+)\\)", "");
                    val values_split: Array[String] = sql_query.split("VALUES")
                    val table_name: String = sql_query.split("INTO")(1).trim().split(" ")(0)
                    sql_query = values_split(0) + "VALUES" + values_split(1).replaceFirst("\\(([^\\)]+)\\)", "(" + ("0," * dataframeSizes(table_name)).dropRight(1) + ")");
                    println(sql_query)
                }
                queries.+=(sql_query)
            }
        }
        queries.toList
    }

    /**
     * Loads the query log from the skyserver files into a list of usable SQL queries
     *
     * @return a List of query SQL statements
     */
    def queryWorkloadSkyserver(): List[String] = {
        val queryLog: DataFrame = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
            .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").load(LOG_DIR + "log.csv");
        queryLog.select("statement").rdd.map(r => r(0).asInstanceOf[String]).collect().toList
    }


    /**
     * Loads the query log from the IoT files into a list of usable SQL queries
     *
     * @return a List of query SQL statements
     */
    def queryWorkloadIoT(): List[String] = {
        var queries: ListBuffer[String] = ListBuffer();
        val src: Iterator[String] = Source.fromFile(LOG_DIR + "processed.log").getLines
        for (l <- src) {
            val sql_query: String = l.replace(";", "")
            queries.+=(sql_query)
        }
        queries.toList
    }

    /**
     * Takes a list of query SQL statements and generates the sequence of sets of partial QEPs
     *
     * @param queries sequence of query SQL statements
     * @return sequence of sets of partial QEPs
     */
    def processQueries(queries: List[String]): Seq[Set[LogicalPlan]] = {
        println("Processing queries, found: " + queries.length)
        var logicalPlans: ListBuffer[Set[LogicalPlan]] = ListBuffer();
        for (i <- queries.indices) {
            val query: String = queries(i)
            println(query)
            try {
                println("========== rawPlan ==========")
                val rawPlan: LogicalPlan = sparkSession.sqlContext.sql(query).queryExecution.analyzed
                println(rawPlan)
                println("========== logicalPlan ==========")
                val logicalPlan: LogicalPlan = sparkSession.sessionState.optimizer.execute(rawPlan)
                val afterEliminateSubqueryAliases = EliminateSubqueryAliases(logicalPlan)
                println(afterEliminateSubqueryAliases)
                val subTrees: Set[LogicalPlan] = getPartialQEPs(afterEliminateSubqueryAliases)
                logicalPlans += subTrees
            }
            catch {
                case e: Exception => println(e)
            }
        }
        logicalPlans.toSeq
    }

    /**
     * Encodes each element of each set as a unique id based on subTreeIds
     *
     * @param processedQueries sequence of sets of partial QEPs
     * @return sequence of sets of unique ids
     */

    def encodeQueries(processedQueries: Seq[Set[LogicalPlan]]): Seq[Set[Long]] = {
        println("Encoding sets of partial QEPs, found: " + processedQueries.length)
        var encodedQueries: ListBuffer[Set[Long]] = ListBuffer();
        for (i <- processedQueries.indices) {
            val partialQEPs: Set[LogicalPlan] = processedQueries(i)
            println(partialQEPs)
            var set: Set[Long] = Set[Long]()
            //map each item in each set to its ID
            partialQEPs.foreach(lp => {
                if (subTreeIds.contains(lp)) {
                    set += subTreeIds(lp)
                }
            })
            println(set)
            encodedQueries += set
        }
        encodedQueries.toSeq
    }

    /**
     * Recurse all subtrees of the logical plan, updates the count for each partial QEP
     *
     * @param lp logical plan of the query
     * @return A set of all partial QEPs (subtrees of logical plan) of lp, including lp
     */

    def getPartialQEPs(lp: LogicalPlan): Set[LogicalPlan] = {
        var subTrees: Set[LogicalPlan] = Set();
        subTreeCount(lp) = subTreeCount.getOrElse[Long](lp, 0) + 1
        subTrees += lp
        if (lp.children.nonEmpty) {
            lp.children.foreach(x => {
                subTrees = subTrees ++ getPartialQEPs(x)
            })
        }
        subTrees
    }


    def writeToCSV(encodedQueries: Seq[Set[Long]]): Unit = {
        println("Writing to CSV");

        val size: Int = encodedQueries.length
        val split: Int = (size * 0.8).toInt

        val outputHistory: BufferedWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR + DATA_SET + "_output_first80percent_" + TOP_K + ".csv"))
        val csvWriterHistory: CSVWriter = new CSVWriter(outputHistory)
        csvWriterHistory.writeNext("queryId", "partialQEPId")
        for (i <- 0 until split) {
            for(partialQEP <- encodedQueries(i)) {
                csvWriterHistory.writeNext(i.toString, partialQEP.toString)
            }
        }
        outputHistory.close()


        val outputFuture: BufferedWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR + DATA_SET + "_output_last20percent_" + TOP_K + ".csv"))
        val csvWriterFuture: CSVWriter = new CSVWriter(outputFuture)
        csvWriterFuture.writeNext("queryId", "partialQEPId")
        for (i <- split until size) {
            for(partialQEP <- encodedQueries(i)) {
                csvWriterFuture.writeNext(i.toString, partialQEP.toString)
            }
        }
        outputFuture.close()
    }
}