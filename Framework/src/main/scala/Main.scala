import java.io.File
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, ReturnAnswer, Sort, SubqueryAlias}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import java.io.{BufferedWriter, FileWriter}
import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.util.{SizeEstimator, Utils}

import java.math.{MathContext, RoundingMode}
import java.util.Locale
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType, StringType, StructField, StructType, TimestampType, NullType}

import java.sql.Timestamp
import java.util.concurrent.TimeUnit.NANOSECONDS
import scala.util.Random

object Main {
    val sparkSession: SparkSession = SparkSession.builder
        .appName("Taster")
        .master("local[*]")
        .getOrCreate();
    import sparkSession.implicits._
    import sparkSession.sqlContext.implicits._

    val ROOT_LOGGER: Logger = Logger.getRootLogger
    ROOT_LOGGER.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    val DATA_SET: String = "phonelabs" // "skyserver" | "phonelabs" | "iot"
    val DATA_DIR: String = "./data/";
    val LOG_DIR: String = DATA_DIR + DATA_SET + "/logs/";
    val OUTPUT_DIR: String = DATA_DIR + DATA_SET + "/output/";
    val SCHEMA_DIR: String = DATA_DIR + DATA_SET + "/schema/";
    val USE_DATA_SUBSET: Boolean = false;
    val TOP_K = 5000;
    val FILL_DUMMY_DATA: Boolean = false;
    val NUM_DUMMY_ROWS = 100000;

    val tableCounter: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
    var subTreeCount: mutable.HashMap[LogicalPlan, Long] = new mutable.HashMap[LogicalPlan, Long]()
    var subTreeIds: mutable.HashMap[LogicalPlan, Long] = new mutable.HashMap[LogicalPlan, Long]()
    var subTreeComputationTime: mutable.HashMap[Long, Long] = new mutable.HashMap[Long, Long]()
    var subTreeByteSize: mutable.HashMap[Long, Long] = new mutable.HashMap[Long, Long]()
    var subTreeRowCount: mutable.HashMap[Long, Long] = new mutable.HashMap[Long, Long]()
    var subTreeDependencyGraph: mutable.HashMap[LogicalPlan, Set[LogicalPlan]] = new mutable.HashMap[LogicalPlan, Set[LogicalPlan]]()
    var subTreeIdsDependencyGraph: mutable.HashMap[Long, Set[Long]] = new mutable.HashMap[Long, Set[Long]]()
    var dataframeSizes: Map[String, Int] = Map[String, Int]()

    def main(args: Array[String]): Unit = {

        val start = System.nanoTime()


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

//        subTreeCount.foreach({case (key, value) => {
//            if(value == 1){
//                println(key)
//            }
//        }})

        createTreeIds()

        createTreeIdsDependencyGraph()

        analyzeSubTrees()

        val encodedQueries: Seq[Set[Long]] = encodeQueries(processedQueries)


        val end = System.nanoTime()
        val duration = NANOSECONDS.toMillis(end - start)
        println(s"Total Duration: ${duration} ms")

        writeToCSV(encodedQueries)

        println("============FINISHED STATS============")
        println("Only encoding the top " + TOP_K + " partial QEPs")
        println("Number of queries: " + queries.size)
        println("Total unique partial QEPs: " + subTreeCount.size)
        println("Top 10 frequent partial QEPs:")
        println(subTreeCount.toSeq.sortWith(_._2 > _._2).take(10))
        println("Frequency distribution of top " + TOP_K +" partial QEPs: " + subTreeCount.values.toList.sorted(Ordering.Long.reverse).take(TOP_K))
        println("Number of partial QEPs only occurring once: " + subTreeCount.values.toList.count(_ == 1))

        println("Processed queries: " + processedQueries.length + " out of " + queries.length)
        println("Finished encoding queries: " + processedQueries.length + " out of " + encodedQueries.length)

        val file = new File(OUTPUT_DIR + DATA_SET + "_output_stats_" + TOP_K + ".txt")
        val bw = new BufferedWriter(new FileWriter(file))
        bw.write("============FINISHED STATS============" + "\n")
        bw.write("Only encoding the top " + TOP_K + " partial QEPs" + "\n")
        bw.write("Number of queries: " + queries.size + "\n")
        bw.write("Total unique partial QEPs: " + subTreeCount.size + "\n")
        bw.write("Top 10 frequent partial QEPs:" + "\n")
        bw.write(subTreeCount.toSeq.sortWith(_._2 > _._2).take(10).toString() + "\n")
        bw.write("Frequency distribution of top " + TOP_K +" partial QEPs: " + subTreeCount.values.toList.sorted(Ordering.Long.reverse).take(TOP_K) + "\n")
        bw.write("Number of partial QEPs only occurring once: " + subTreeCount.values.toList.count(_ == 1) + "\n")

        bw.write("Processed queries: " + processedQueries.length + " out of " + queries.length + "\n")
        bw.write("Finished encoding queries: " + processedQueries.length + " out of " + encodedQueries.length + "\n")
        bw.close()



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
     * Transforms the treeDependnecyGraph to one of ids instead of logical plans
     */
    def createTreeIdsDependencyGraph(): Unit = {
        subTreeDependencyGraph foreach {
            case (lp, subTreeSet) =>
                if (subTreeIds.contains(lp)) {
                    val subTreeIdsSet = subTreeSet.filter(p => subTreeIds.contains(p)).map(p => subTreeIds(p))
                    subTreeIdsDependencyGraph(subTreeIds(lp)) = subTreeIdsSet
                }
        }
    }

    /**
     * Evaluates each unique partial QEP in subTreeIds to obtain the running times
     */

    def analyzeSubTrees(): Unit = {
        subTreeIds foreach {
            case (lp, id) =>
                println("========== Running Partial QEP ==========")
                println(lp)
                val start = System.nanoTime()
                val res = sparkSession.sessionState.executePlan(lp).toRdd
                val end = System.nanoTime()
                val duration = NANOSECONDS.toMillis(end - start)
                println(s"Time taken: ${duration} ms")
                subTreeComputationTime(id) = duration

                val calc_rowcount = res.count()
                val calc_bytes = getRDDSize(res)
                val est_bytes = sparkSession.sessionState.executePlan(
                    lp).optimizedPlan.stats.sizeInBytes
                val est_rowcount = sparkSession.sessionState.executePlan(
                    lp).optimizedPlan.stats.rowCount.getOrElse(None)
                println("est sizeInBytes=" + bytesToString(est_bytes) + ", calc sizeInBytes=" + bytesToString(calc_bytes) + ", est rowCount=" + est_rowcount + ", calc rowCount=" + calc_rowcount)
                subTreeByteSize(id) = calc_bytes
                subTreeRowCount(id) = calc_rowcount
        }
    }

    def randomStringGen(length: Int): String = scala.util.Random.alphanumeric.take(length).mkString


    def zeroRowBySchema(schema: StructType): Seq[Any] = {
        val types = schema.map((f: StructField) => {
            if (f.name != "deleted_at") {
                f.dataType
            } else {
                NullType
            }
        })
        val dummyrow = types map {
            case IntegerType => 0
            case StringType => "0"
            case TimestampType => new Timestamp(System.currentTimeMillis());
            case BooleanType => Random.nextBoolean()
            case NullType => null
            case _ => "0"
        }
        dummyrow
    }

    def randomRowBySchema(schema: StructType): Seq[Any] = {
        val types = schema.map((f: StructField) => {
            if (f.name != "deleted_at") {
                f.dataType
            } else {
                NullType
            }
        })
        val dummyrow = types map {
            case IntegerType => Random.nextInt(NUM_DUMMY_ROWS)
            case StringType => Random.nextInt(NUM_DUMMY_ROWS).toString
            case TimestampType => new Timestamp(System.currentTimeMillis());
            case BooleanType => Random.nextBoolean()
            case NullType => null
            case _ => "0"
        }
        dummyrow
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
                    var loaded: DataFrame = sparkSession.sqlContext
                        .read.format("com.databricks.spark.csv")
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .option("delimiter", delimiter)
                        .option("nullValue", "null")
                        .load(SCHEMA_DIR + "/" + files(i).getName)

                    if (FILL_DUMMY_DATA) {
                        var lstrows = Seq.fill(10)(zeroRowBySchema(loaded.schema)).toList.map { x => Row(x: _*) }
                        lstrows ++= Seq.fill(NUM_DUMMY_ROWS - 10)(randomRowBySchema(loaded.schema)).toList.map { x => Row(x: _*) }
                        val rdd = sparkSession.sparkContext.parallelize(lstrows)
                        val dummy_df = sparkSession.createDataFrame(rdd, loaded.schema)
                        loaded = loaded.union(dummy_df)
                    }
                    dataframes += (filename -> loaded)
                    sparkSession.createDataFrame(loaded.rdd, loaded.schema).createOrReplaceTempView(filename)
                } catch {
                    case e: Exception =>
                        println("Error occurred for: " + files(i).getName)
                        println(e)
                }
            }
        }

        sparkSession.catalog.listTables("default").foreach(t => println(t))

//        dataframes.keys.foreach((k) => {
//            sparkSession.table(k).printSchema()
//            sparkSession.table(k).cache().count()
//            println(k, sparkSession.table(k).count())
//            println(sparkSession.table(k).head(5).mkString("Array(", ", ", ")"))
//        })

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

        var src: Iterator[String] = Source.fromFile(LOG_DIR + "2747d54967a32fc95945671b930d57c1d5a9ac02_log_fixed.csv").getLines
        src.take(1).next
        for (l <- src) {
//            println(l.split(";")(3)) // can cause array out of bounds exception for some queries
            // Filter for first day and first hour only to limit number of queries
            if ((l.contains("2015-03-01") ||l.contains("2015-03-02") || l.contains("2015-03-03") || l.contains("2015-03-04") || l.contains("2015-03-05")) && !l.contains("sqlite_master") && !l.contains("INSERT") && l.contains("SELECT") && !l.contains("DELETE") && !l.contains("REPLACE") && !l.contains("UPSERT") && !l.contains("UPDATE")) {
//            if ((l.contains("2015-03-01")) && !l.contains("sqlite_master") && !l.contains("INSERT") && l.contains("SELECT") && !l.contains("DELETE") && !l.contains("REPLACE") && !l.contains("UPSERT") && !l.contains("UPDATE")) {
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
        val src: Iterator[String] = Source.fromFile(LOG_DIR + "processed_fixed.log").getLines
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
            println("Processing: " + i + "/" + queries.length)
            val query: String = queries(i)
            println(query)
            try {
                println("========== rawPlan ==========")
                val rawPlan: LogicalPlan = sparkSession.sqlContext.sql(query).queryExecution.analyzed
                println(rawPlan)
                println("========== logicalPlan ==========")
                //Same as the optimized plan
                val logicalPlan: LogicalPlan = sparkSession.sessionState.optimizer.execute(rawPlan)
                val afterEliminateSubqueryAliases = EliminateSubqueryAliases(logicalPlan)
                println(afterEliminateSubqueryAliases)
                val subTrees: Set[LogicalPlan] = getPartialQEPs(afterEliminateSubqueryAliases)
                logicalPlans += subTrees
            }
            catch {
                case e: Exception =>
            }
        }
        logicalPlans.toSeq
    }

    def listMean(list:List[Long]): Double = if(list.isEmpty) 0 else list.sum.toDouble/list.size

    /**
     * Encodes each element of each set as a unique id based on subTreeIds
     *
     * @param processedQueries sequence of sets of partial QEPs
     * @return sequence of sets of unique ids
     */

    def encodeQueries(processedQueries: Seq[Set[LogicalPlan]]): Seq[Set[Long]] = {
        var encodingTimes: ListBuffer[Long] = ListBuffer();

        println("Encoding sets of partial QEPs, found: " + processedQueries.length)
        var encodedQueries: ListBuffer[Set[Long]] = ListBuffer();
        for (i <- processedQueries.indices) {
            println("Encoding: " + i + "/" + processedQueries.length)
            val start = System.nanoTime()
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
            val end = System.nanoTime()
            val duration = NANOSECONDS.toMillis(end - start)
            encodingTimes += duration
        }
        println("Encoding queries took: " + encodingTimes.toList.sum + " ms in total")
        println("Average encoding time per query: " + listMean(encodingTimes.toList) + " ms")
        encodedQueries.toSeq
    }


    def bytesToString(size: Long): String = bytesToString(BigInt(size))

    def bytesToString(size: BigInt): String = {
        val EB = 1L << 60
        val PB = 1L << 50
        val TB = 1L << 40
        val GB = 1L << 30
        val MB = 1L << 20
        val KB = 1L << 10

        if (size >= BigInt(1L << 11) * EB) {
            // The number is too large, show it in scientific notation.
            BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B"
        } else {
            val (value, unit) = {
                if (size >= 2 * EB) {
                    (BigDecimal(size) / EB, "EB")
                } else if (size >= 2 * PB) {
                    (BigDecimal(size) / PB, "PB")
                } else if (size >= 2 * TB) {
                    (BigDecimal(size) / TB, "TB")
                } else if (size >= 2 * GB) {
                    (BigDecimal(size) / GB, "GB")
                } else if (size >= 2 * MB) {
                    (BigDecimal(size) / MB, "MB")
                } else if (size >= 2 * KB) {
                    (BigDecimal(size) / KB, "KB")
                } else {
                    (BigDecimal(size), "B")
                }
            }
            "%.1f %s".formatLocal(Locale.US, value, unit)
        }
    }

    def getRDDSize(rdd: RDD[InternalRow]) : Long = {
        var rddSize = 0L
        val rows = rdd.collect()
        for (k <- rows.indices) {
            val row = rows(k)
            val len = row.numFields
            rddSize += SizeEstimator.estimate(row)
        }
        rddSize
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
        subTreeDependencyGraph(lp) = subTrees.filter((p: LogicalPlan) => p != lp)
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


        val map: BufferedWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR + DATA_SET + "_output_mapping_" + TOP_K + ".csv"))
        val csvWriterMap: CSVWriter = new CSVWriter(map, ';')
        csvWriterMap.writeNext("partialQEPId", "partialQEP")

        subTreeIds foreach {case (key, value) => csvWriterMap.writeNext(value.toString, key.toString())}

        map.close()


        val uniques: BufferedWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR + DATA_SET + "_output_uniques_" + TOP_K + ".csv"))
        val csvWriterUniques: CSVWriter = new CSVWriter(uniques, ';')
        csvWriterUniques.writeNext("frequency", "partialQEP")

        subTreeCount foreach {case (key, value) => {
            if(value == 1) {
                csvWriterUniques.writeNext(value.toString, key.toString())
            }

        }}

        uniques.close()

        val comptime: BufferedWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR + DATA_SET + "_output_comptime_"+ NUM_DUMMY_ROWS +"rows_" + TOP_K + ".csv"))
        val csvWriterComptime: CSVWriter = new CSVWriter(comptime, ';')
        csvWriterComptime.writeNext("partialQEPId", "computationTime", "byteSize", "rowCount")

        subTreeComputationTime foreach {case (key, value) =>
            csvWriterComptime.writeNext(key.toString, value.toString, subTreeByteSize(key).toString, subTreeRowCount(key).toString)
        }

        comptime.close()


        val dependencyGraph: BufferedWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR + DATA_SET + "_output_dependency_graph_" + TOP_K + ".csv"))
        val csvWriterDependencyGraph: CSVWriter = new CSVWriter(dependencyGraph, ';')
        csvWriterDependencyGraph.writeNext("partialQEP", "impliedPartialQEPs")

        subTreeIdsDependencyGraph foreach {case (key: Long, value: Set[Long]) => csvWriterDependencyGraph.writeNext(key.toString, value.toString.replace("Set(", "").replace(")", ""))}

        dependencyGraph.close()
    }
}