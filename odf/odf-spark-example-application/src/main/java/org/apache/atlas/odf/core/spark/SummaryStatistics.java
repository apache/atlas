/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.odf.core.spark;

import org.apache.atlas.odf.api.spark.SparkUtils;
import org.apache.spark.SparkFiles;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SummaryStatistics {
	static Logger logger = Logger.getLogger(SummaryStatistics.class.getName());
	private static final String CSV_FILE_PARAMETER = "-dataFile=";
	// The following constant is defined in class DiscoveryServiceSparkEndpoint but is duplicated here to avoid dependencies to the ODF code:
	private static final String ANNOTATION_PROPERTY_COLUMN_NAME = "ODF_ANNOTATED_COLUMN";

	// The main method is only available for testing purposes and is not called by ODF
	public static void main(String[] args) {
		logger.log(Level.INFO, "Running spark launcher with arguments: " + args[0]);
		if ((args[0] == null) || (!args[0].startsWith(CSV_FILE_PARAMETER))) {
			System.out.println(MessageFormat.format("Error: Spark Application Parameter '{0}' is missing.", CSV_FILE_PARAMETER));
			System.exit(1);
		}
		String dataFilePath = SparkFiles.get(args[0].replace(CSV_FILE_PARAMETER, ""));
		logger.log(Level.INFO, "Data file path is " + dataFilePath);

		// Create Spark session
		SparkSession spark = SparkSession.builder().master("local").appName("ODF Spark example application").getOrCreate();

		// Read CSV file into data frame
		Dataset<Row> df = spark.read()
		    .format("com.databricks.spark.csv")
		    .option("inferSchema", "true")
		    .option("header", "true")
		    .load(dataFilePath);

		// Run actual job and print result
		Map<String, Dataset<Row>> annotationDataFrameMap = null;
		try {
			annotationDataFrameMap = processDataFrame(spark, df, args);
		} catch (Exception e) {
			logger.log(Level.INFO, MessageFormat.format("An error occurred while processing data set {0}:", args[0]), e);
		} finally {
			// Close and stop spark context
			spark.close();
			spark.stop();
		}
		if (annotationDataFrameMap == null) {
			System.exit(1);
		} else {
			// Print all annotationDataFrames for all annotation types to stdout
			for (Map.Entry<String, Dataset<Row>> entry : annotationDataFrameMap.entrySet()) {
				logger.log(Level.INFO, "Result data frame for annotation type " + entry.getKey() + ":");
				entry.getValue().show();
			}
		}
	}

	// The following method contains the actual implementation of the ODF Spark discovery service
	public static Map<String,Dataset<Row>> processDataFrame(SparkSession spark, Dataset<Row> df, String[] args) {
		logger.log(Level.INFO, "Started summary statistics Spark application.");
		Map<String, Dataset<Row>> resultMap = new HashMap<String, Dataset<Row>>();

		// Print input data set
		df.show();

		// Create column annotation data frame that contains basic data frame statistics
		Dataset<Row> dfStatistics = df.describe();

		// Rename "summary" column to ANNOTATION_PROPERTY_COLUMN_NAME
		String[] columnNames = dfStatistics.columns();
		columnNames[0] = ANNOTATION_PROPERTY_COLUMN_NAME;
		Dataset<Row> summaryStatistics =  dfStatistics.toDF(columnNames);
		summaryStatistics.show();
		String columnAnnotationTypeName = "SparkSummaryStatisticsAnnotation";

		// Transpose table to turn it into format required by ODF
		Dataset<Row> columnAnnotationDataFrame = SparkUtils.transposeDataFrame(spark, summaryStatistics);
		columnAnnotationDataFrame.show();

		// Create table annotation that contains the data frame's column count
		String tableAnnotationTypeName = "SparkTableAnnotation";
		Dataset<Row> tableAnnotationDataFrame = columnAnnotationDataFrame.select(new Column("count")).limit(1);
		tableAnnotationDataFrame.show();

		// Add annotation data frames to result map
		resultMap.put(columnAnnotationTypeName, columnAnnotationDataFrame);
		resultMap.put(tableAnnotationTypeName, tableAnnotationDataFrame);

		logger.log(Level.INFO, "Spark job finished.");
		return resultMap;
	}
}
