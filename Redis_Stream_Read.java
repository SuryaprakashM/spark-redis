

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;



public class RedisRead {

	public static void main(String[] args) throws Exception {

		try {

			SparkSession spark = SparkSession.builder().appName("RedisStreamExample")
					.config("spark.redis.host", "**.*.*.***").config("spark.redis.port", "6379").getOrCreate();		

			StructType UserSchema5 = new StructType(new StructField[] {

					DataTypes.createStructField("make", DataTypes.StringType, false),
					DataTypes.createStructField("year", DataTypes.IntegerType, false),
					DataTypes.createStructField("serialnum", DataTypes.IntegerType, false),
					DataTypes.createStructField("model", DataTypes.StringType, false) });

			org.apache.spark.sql.catalyst.encoders.ExpressionEncoder<org.apache.spark.sql.Row> enc5 = org.apache.spark.sql.catalyst.encoders.RowEncoder
					.apply(UserSchema5);

			Dataset<Row> RedisData = 
					spark.readStream()
					.format("redis")
					.option("stream.keys", "carsstream")
					.schema(UserSchema5).load();

			
			RedisData.show();
			
		} catch (Exception e) {
			System.out.println(e);
		}
	}

}