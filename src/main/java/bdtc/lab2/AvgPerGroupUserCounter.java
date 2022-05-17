package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.temporal.ChronoField.YEAR;

@AllArgsConstructor
@Slf4j
public class AvgPerGroupUserCounter {

    /**
     * Функция подсчета количества сообщений в группе на пользователя
     * Парсит строку из PostgreSQL, в т.ч. отправителя,получателя,дата,сообщение(табдлица messages)
     * @param groupMap - входная таблица-справочник группа-пользователь для анализа
     * @param messages - входная таблица (содержит поля отправителя,получателя,дата,сообщение(табдлица messages))
     * @return результат подсчета в формате JavaRDD
     */
    public static JavaRDD<Tuple2<String,Integer>> countAvgPerGroupUser(JavaRDD<String> messages,JavaRDD<String> groupMap) {
        //split by row using newline symbol)
        JavaRDD<String> groupRows = groupMap.map(rows -> Arrays.toString(rows.split("\n")));
        JavaPairRDD<String,String> groupAndUserPair = groupRows.mapToPair(map -> {
           String[] userAndGroup = map.split(",");
           return new Tuple2<String,String>(userAndGroup[0],userAndGroup[1]);
        });
        List<Tuple2<String,String>> listUserAndGroup =  groupAndUserPair.collect();

        JavaRDD<String> words = messages.map(s -> Arrays.toString(s.split("\n")));
        JavaPairRDD<String,Integer> msg  = words.mapToPair(map -> {
            String[] field = map.split(",");
            String group1 = "";
            String group2 = "";
            for (Tuple2 entry: listUserAndGroup) {
                if(entry._1().equals(field[0])) {
                    group1 = (String) entry._2();
                }
            }
            return new Tuple2<String,Integer>(group1+"|"+field[0],1);
        });

        JavaPairRDD<String,Integer> msg2  = words.mapToPair(map -> {
            String[] field = map.split(",");
            String group2 = "";
            for (Tuple2 entry: listUserAndGroup) {
                if(entry._1().equals(field[1])) {
                    group2 = (String) entry._2();
                }
            }
            return new Tuple2<String,Integer>(group2+"|"+field[1],1);
        });
        JavaPairRDD<String,Integer> grouped = msg.union(msg2);

        // Группирует по значениям часа и уровня логирования Group by sender
        JavaPairRDD<String, Integer> t = grouped.reduceByKey((a,b) -> {
           return a + b;
        });
        JavaRDD<Tuple2<String,Integer>> resultRDD = t.map(j -> {
            return new Tuple2<String,Integer>(j._1(),j._2());
        });
        log.info("===========RESULT=========== ");

        return resultRDD;
    }

}
