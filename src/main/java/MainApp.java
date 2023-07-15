import org.apache.spark.sql.*;

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
        //dataset1.show(10,false);

        //tema 2 - functii basic - select, filter, groupBY, orderBy, count
        //dataset1.printSchema();
        //dataset1.select("Mobility Duration").show();
        //dataset1.select(new Column("Sending Country Code").toString().toLowerCase(),"Participant Age").show();
        //dataset1.filter(new Column("Participant Age").gt(33)).show();
        //dataset1.groupBy("Participant Age").count().show();

        dataset = dataset.filter(functions.col("Receiving Country Code").isin("RO","UK","LT"));

        dataset.groupBy("Receiving Country Code","Sending Country Code")
                .count().withColumnRenamed("count","Number Of Students")
                .orderBy(new Column("Receiving Country Code").desc())
                .show(60);

        //tema 3  Transfer date Dataset -> DB
        String url="jdbc:oracle:thin:@localhost:1521:xe";
        String driver="oracle.jdbc.driver.OracleDriver";
        String user="exemplu_user";
        String password="exemplu_user";
        String table="Countries";

        dataset
                .write()
                .mode(SaveMode.Overwrite)
                .format("jdbc")
                .option("driver",driver)
                .option("url",url)
                .option("dbtable",table)
                .option("user",user)
                .option("password",password)
                .save();

        //Romania table
        Dataset<Row> datasetRO=spark.read().option("header",true).csv("C:/Users/alex/source/laburi_java/IBM_try_javaNOU/src/main/resources/erasmus.csv");
        datasetRO = datasetRO.filter(functions.col("Receiving Country Code").equalTo("RO"));

        datasetRO = datasetRO.groupBy("Receiving Country Code","Sending Country Code")
                .count().withColumnRenamed("count","Number Of Students")
                .orderBy(new Column("Receiving Country Code").desc());

        datasetRO.show();

        String tableRO="Romania";
        datasetRO
                .write()
                .mode(SaveMode.Overwrite)
                .format("jdbc")
                .option("driver",driver)
                .option("url",url)
                .option("dbtable",tableRO)
                .option("user",user)
                .option("password",password)
                .save();

        //United Kingdom table
        Dataset<Row> datasetUK=spark.read().option("header",true).csv("C:/Users/alex/source/laburi_java/IBM_try_javaNOU/src/main/resources/erasmus.csv");
        datasetUK = datasetUK.filter(functions.col("Receiving Country Code").equalTo("UK"));

        datasetUK = datasetUK.groupBy("Receiving Country Code","Sending Country Code")
                .count().withColumnRenamed("count","Number Of Students")
                .orderBy(new Column("Receiving Country Code").asc());

        datasetUK.show();

        String tableUK="United_Kingdom";
        datasetUK
                .write()
                .mode(SaveMode.Overwrite)
                .format("jdbc")
                .option("driver",driver)
                .option("url",url)
                .option("dbtable",tableUK)
                .option("user",user)
                .option("password",password)
                .save();

        //Tabela Lithuania
        Dataset<Row> datasetLT=spark.read().option("header",true).csv("C:/Users/alex/source/laburi_java/IBM_try_javaNOU/src/main/resources/erasmus.csv");
        datasetLT = datasetLT.filter(functions.col("Receiving Country Code").equalTo("LT"));

        datasetLT = datasetLT.groupBy("Receiving Country Code","Sending Country Code")
                .count().withColumnRenamed("count","Number Of Students")
                .orderBy(new Column("Receiving Country Code").desc());

        datasetLT.show();

        String tableLT="Lithuania";
        datasetLT
                .write()
                .mode(SaveMode.Overwrite)
                .format("jdbc")
                .option("driver",driver)
                .option("url",url)
                .option("dbtable",tableLT)
                .option("user",user)
                .option("password",password)
                .save();
    }
}
