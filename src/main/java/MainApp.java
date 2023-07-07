import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MainApp {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> dataset=spark.read().csv("C:/Users/alex/source/laburi_java/IBM_try_javaNOU/src/main/resources/erasmus.csv");
        dataset.show(10,false);
    }
}
