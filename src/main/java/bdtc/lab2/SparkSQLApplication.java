package bdtc.lab2;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * группа-среднее количество сообщений на одного пользователя в групп
 */
@Slf4j
public class SparkSQLApplication {

    /**
     * @param args - args[0]: входной файл, args[1] - выходная папка
     */
    public static void main(String[] args) {
        if (args.length < 3) {
            throw new RuntimeException("Usage: java -jar SparkSQLApplication.jar input.file outputDirectory");
        }

        log.info("Appliction started!");
        log.debug("Application started");
        SparkSession sc = SparkSession
                .builder()
                .master("local")
                .appName("SparkSQLApplication")
                .getOrCreate();
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Spark RDD Example using Java");
        sparkConf.setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sparkContext.textFile(args[0]);;
        JavaRDD<String> lines2 = sparkContext.textFile(args[1]);

        log.info("===============COUNTING...================");
        JavaRDD<Tuple2<String,Integer>> result = AvgPerGroupUserCounter.countAvgPerGroupUser(lines,lines2);
        log.info("============SAVING FILE TO " + args[2] + " directory============");
        result.saveAsTextFile(args[2]);
    }
}
