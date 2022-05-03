package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

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

    // Формат времени  - н-р, 'Oct 26 13:54:06'
    private static DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .appendPattern("MMM dd HH:mm:ss")
            .parseDefaulting(YEAR, 2018)
            .toFormatter();

    static HashMap<String, String> groupAndUserMap;
    {
        Stream.of(new String[][]{
                {"group1", "user1"},
                {"group2", "user2"},
                {"group3", "user3"},
                {"group4", "user4"},
                {"group5", "user5"},
                {"group6", "user6"},
                {"group7", "user7"},
                {"group8", "user8"}
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]));
    }

    /**
     * Функция подсчета количества логов разного уровня в час.
     * Парсит строку лога, в т.ч. уровень логирования и час, в который событие было зафиксировано.
     * @param inputDataset - входной DataSet для анализа
     * @return результат подсчета в формате JavaRDD
     */
    public static JavaRDD<Row> countAvgPerGroupUser(Dataset<String> inputDataset) {
        //split by row using newline symbol)
        Dataset<String> words = inputDataset.map(s -> Arrays.toString(s.split("\n")), Encoders.STRING());
        int group1Count,group2Count,group3Count,group4Count,group5Count,group6Count,group7Count,group8Count;
        //split by comma
        Dataset<UserRecord> userDataset = words.map(s -> {
            String[] fields = s.split(",");
            LocalDateTime date = LocalDateTime.parse(fields[2], formatter);
            groupAndUserMap.forEach((k, v) -> v.equals(fields[0]));
            if( groupAndUserMap.get("group1").equals(fields[0]) ){
                for (Map.Entry<String, String> entry : groupAndUserMap.entrySet()) {
                    if(entry.getValue().equals(fields[0])&&entry.getKey().equals("group1")){

                    } else if(entry.getValue().equals(fields[0])&&entry.getKey().equals("group2")){

                    } else if(entry.getValue().equals(fields[0])&&entry.getKey().equals("group3")){

                    } else if(entry.getValue().equals(fields[0])&&entry.getKey().equals("group4")){

                    } else if(entry.getValue().equals(fields[0])&&entry.getKey().equals("group5")){

                    } else if(entry.getValue().equals(fields[0])&&entry.getKey().equals("group6")){

                    } else if(entry.getValue().equals(fields[0])&&entry.getKey().equals("group7")){

                    } else if(entry.getValue().equals(fields[0])&&entry.getKey().equals("group8")){

                    }
                }//groupAndUserMap.containsKey()
            }
            groupAndUserMap.get("");
            return new UserRecord(fields[0], fields[1], date, fields[3]);
            }, Encoders.bean(UserRecord.class))
                .coalesce(1);

        // Группирует по значениям часа и уровня логирования Group by sender
        Dataset<Row> t = userDataset.groupBy("senderUser")
                .count()
                .toDF("senderUser", "receiverUser", "datetime", "msg")
                // сортируем по времени лога - для красоты
                .sort(functions.avg("senderUser"));
        List list = t.collectAsList();

        log.info("===========RESULT=========== ");
        t.show();
        return t.toJavaRDD();
    }

}
