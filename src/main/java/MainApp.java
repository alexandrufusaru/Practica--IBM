import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MainApp {
    public static void main(String[] args) {
        //tema 1 - creare sesiune spark + citire si afisare csv
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> dataset=spark.read().option("header",true).csv("C:/Users/alex/source/laburi_java/IBM_try_javaNOU/src/main/resources/erasmus.csv");
        //dataset.show(10,false);

        //tema 2 - functii basic - select, filter, groupBY, orderBy, count
        //dataset.printSchema();
        //dataset.select("Mobility Duration").show();
        //dataset.select(new Column("Sending Country Code").toString().toLowerCase(),"Participant Age").show();
        //dataset.filter(new Column("Participant Age").gt(33)).show();
        //dataset.groupBy("Participant Age").count().show();
        dataset.groupBy("Receiving Country Code","Sending Country Code").count().withColumnRenamed("count","Number Of Students").orderBy(new Column("Receiving Country Code").desc()).show();
    }
}
