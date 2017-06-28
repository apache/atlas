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
package org.apache.atlas.odf.api.spark;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResult;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.Utils;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceSparkEndpoint;
import org.apache.atlas.odf.api.discoveryservice.datasets.DataSetContainer;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.metadata.models.CachedMetadataStore;
import org.apache.atlas.odf.api.metadata.models.JDBCConnection;
import org.apache.atlas.odf.api.metadata.models.JDBCConnectionInfo;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.api.metadata.models.Column;
import org.apache.atlas.odf.api.metadata.models.Connection;
import org.apache.atlas.odf.api.metadata.models.DataFile;
import org.apache.atlas.odf.api.metadata.models.RelationalDataSet;
import org.apache.atlas.odf.api.metadata.models.Table;
import org.apache.atlas.odf.api.metadata.models.ProfilingAnnotation;

/**
 * Provides a number of helper methods, mainly for working with Spark data frames.
 * 
 *
 */

public class SparkUtils {
	static Logger logger = Logger.getLogger(SparkUtils.class.getName());

    /**
     * Creates a Spark data frame from a data set reference stored in a data set container.
     * 
     * @param sc Current Spark context
     * @param request dsc Data set container that keeps the reference to the input data set
     * @return Resulting Spark data frame
     */
	public static Dataset<Row> createDataFrame(SparkSession spark, DataSetContainer dsc, MetadataStore mds) {
		Dataset<Row> df = null;
		MetaDataObject ds = dsc.getDataSet();
		if (ds instanceof DataFile) {
			DataFile dataFile = (DataFile) ds;
			logger.log(Level.INFO, MessageFormat.format("Reading DataFile {0} from URL {1}.",
					new Object[] { dataFile.getName(), dataFile.getUrlString() }));
			df = spark.read().format("csv").option("inferSchema", "true")
					.option("header", "true").load(dataFile.getUrlString());
		} else if (ds instanceof Table) {
			Table table = (Table) ds;
			MetadataStore availableMetadataStore;
			if (mds.testConnection() == MetadataStore.ConnectionStatus.OK) {
				availableMetadataStore = mds;
			} else if (dsc.getMetaDataCache() != null) {
				availableMetadataStore = new CachedMetadataStore(dsc.getMetaDataCache()); 
			} else {
				throw new RuntimeException("Discovery service has no access to the metadata store and no metadata cache is available.");
			}
			JDBCConnectionInfo connectionInfo = (JDBCConnectionInfo) availableMetadataStore.getConnectionInfo(table);
			List<Connection> connections = connectionInfo.getConnections();
			if (connections == null || connections.isEmpty()) {
				// No connection information is attached to the relational table that was passed to the discovery service.
				// This is typically caused by the fact that the Spark discovery service cannot access the ODF metadata API in order to retrieve cached objects
				String msg = "Spark discovery service cannot access the ODF metadata API. Make sure that the ODF REST API is accessible from the discovery service running on the Spark cluster.";
				logger.log(Level.SEVERE, msg);
				throw new RuntimeException(msg);
			}
			JDBCConnection jdbcConnection = null;
			for (Connection connection : connections) {
				if (connection instanceof JDBCConnection) {
					jdbcConnection = (JDBCConnection) connection;
					break;
				}
			}
			String driver = null;
			try {
				// Get JDBC driver class name needed for populating DataFrame
				// below
				driver = DriverManager.getConnection(jdbcConnection.getJdbcConnectionString(), jdbcConnection.getUser(),
						jdbcConnection.getPassword()).getClass().getName();
				logger.log(Level.INFO, MessageFormat.format("JDBC driver class name is {0}.", driver));
			} catch (SQLException e) {
				String msg = MessageFormat.format("Error connecting to JDBC data source {0}: ",
						jdbcConnection.getJdbcConnectionString());
				logger.log(Level.WARNING, msg, e);
				throw new RuntimeException(msg + Utils.exceptionString(e));
			}
			String schemaName = connectionInfo.getSchemaName();
			String url = jdbcConnection.getJdbcConnectionString() + ":currentSchema=" + schemaName + ";user="
					+ jdbcConnection.getUser() + ";password=" + jdbcConnection.getPassword() + ";";
			String dbtable = schemaName + "." + table.getName();
			String msg = "Using JDBC parameters url: {0}, dbtable: {1}, driver: {2} to connect to DB2 database.";
			logger.log(Level.INFO, MessageFormat.format(msg, new Object[] { url, dbtable, driver }));
			Map<String, String> options = new HashMap<String, String>();
			options.put("url", url);
			options.put("dbtable", dbtable);
			options.put("driver", "com.ibm.db2.jcc.DB2Driver");
			df = spark.read().format("jdbc").options(options).load();
		}
		return df;
	}

    /**
     * Generates ODF annotations from a annotation data frames. 
     * 
     * @param container Data set container that contains the reference to the data set to be annotated
     * @param annotationDataFrameMap Maps the annotation types to be created with the annotation data frames that contain the actual annotation data
     * @return Result object that contains a list of ODF annotations
     */
	public static DiscoveryServiceResult createAnnotationsFromDataFrameMap(DataSetContainer container, Map<String, Dataset<Row>> annotationDataFrameMap, MetadataStore mds) throws RuntimeException {
		RelationalDataSet tab = (RelationalDataSet) container.getDataSet();
		DiscoveryServiceResult result = new DiscoveryServiceResult();

		// Map input table columns to metadata object references
		Map<String, MetaDataObjectReference> columnReferencesByName = new HashMap<>();

		List<Column> colList ;
		if (mds.testConnection() == MetadataStore.ConnectionStatus.OK) {
			colList = mds.getColumns(tab);
		} else if (container.getMetaDataCache() != null) {
			CachedMetadataStore cacheReader = new CachedMetadataStore(container.getMetaDataCache());
			colList = cacheReader.getColumns(tab);
		} else {
			throw new RuntimeException("Discovery service has no access to the metadata store and no metadata cache is available.");
		}

		for (MetaDataObject colMDO : colList) {
			Column oMColumn = (Column) colMDO;
			columnReferencesByName.put(oMColumn.getName(), oMColumn.getReference());
		}

		List<Annotation> annotations = new ArrayList<>();
		Dataset<Row> df = null;
		for (Map.Entry<String, Dataset<Row>> entry : annotationDataFrameMap.entrySet()) {
			String annotationType = entry.getKey();
			df = entry.getValue();
			String columnToBeAnnotated = null;
			int rowNumber = 0;
			try {
				List<Row> rows = df.collectAsList();
				String[] columnNames = df.columns();
				StructType st = df.schema();

				for (rowNumber = 0; rowNumber < rows.size(); rowNumber++) {
					if (columnNames[0].equals(DiscoveryServiceSparkEndpoint.ANNOTATION_PROPERTY_COLUMN_NAME)) {
						// Generate column annotations by mapping DataFrame
						// table column values to annotation properties
						// Column ANNOTATION_PROPERTY_COLUMN_NAME represents the
						// column to be annotated
						columnToBeAnnotated = rows.get(rowNumber).getString(0);
						MetaDataObjectReference annotatedColumn = columnReferencesByName.get(columnToBeAnnotated);
						if (annotatedColumn != null) {
							logger.log(Level.FINE, MessageFormat.format("Annotating column {0}:", columnToBeAnnotated));
							annotations.add((Annotation) getAnnotation(st, columnNames, annotationType, rows.get(rowNumber),
									annotatedColumn));
						} else {
							logger.log(Level.FINE, "Column " + columnToBeAnnotated
									+ " returned by the Spark service does not match any column of the input data set.");
						}
					} else {
						// Creating table annotations
						logger.log(Level.INFO,
								MessageFormat.format(
										"Data frame does not contain column {0}. Creating table annotations.",
										DiscoveryServiceSparkEndpoint.ANNOTATION_PROPERTY_COLUMN_NAME));
						annotations.add((Annotation) getAnnotation(st, columnNames, annotationType, rows.get(rowNumber),
								container.getDataSet().getReference()));
					}
				}
			} catch (JSONException exc) {
				String msg = MessageFormat.format(
						"Error processing results returned by DataFrame row {0} column {1}. See ODF application lof for details.",
						new Object[] { rowNumber, columnToBeAnnotated });
				logger.log(Level.WARNING, msg);
				throw new RuntimeException(msg, exc);
			}
		}
		result.setAnnotations(annotations);
		return result;
	}

    /**
     * Creates a single ODF annotation from a row of input data. 
     * 
     * @param st Data types of the annotation attributes 
     * @param columnNames Names of the annotation attributes
     * @param row Input data that represents the values of the annotation attributes  
     * @return A single ODF annotation object
     */
	public static Annotation getAnnotation(StructType st, String[] columnNames, String annotationType, Row row,
			MetaDataObjectReference annotatedObject) throws JSONException {
		ProfilingAnnotation an = new ProfilingAnnotation();
		an.setAnnotationType(annotationType);
		an.setProfiledObject(annotatedObject);
		JSONObject jsonProperties = new JSONObject();
		for (int j = 0; j < columnNames.length; j++) {
			if (!columnNames[j].equals(DiscoveryServiceSparkEndpoint.ANNOTATION_PROPERTY_COLUMN_NAME)) {
				if (columnNames[j].equals(DiscoveryServiceSparkEndpoint.ANNOTATION_SUMMARY_COLUMN_NAME)) {
					an.setSummary(row.getString(j));
				} else {
					String annotationPropertyName = columnNames[j];
					DataType dataType = st.apply(annotationPropertyName).dataType();
					if (dataType == DataTypes.IntegerType) {
						jsonProperties.put(annotationPropertyName, row.getInt(j));
					} else if (dataType == DataTypes.DoubleType) {
						jsonProperties.put(annotationPropertyName, row.getDouble(j));
					} else if (dataType == DataTypes.BooleanType) {
						jsonProperties.put(annotationPropertyName, row.getBoolean(j));
					} else if (dataType == DataTypes.FloatType) {
						jsonProperties.put(annotationPropertyName, row.getFloat(j));
					} else if (dataType == DataTypes.LongType) {
						jsonProperties.put(annotationPropertyName, row.getLong(j));
					} else if (dataType == DataTypes.ShortType) {
						jsonProperties.put(annotationPropertyName, row.getShort(j));
					} else {
						// Return all other data types as String
						jsonProperties.put(annotationPropertyName, row.getString(j));
					}
					logger.log(Level.FINE, "Set attribute " + annotationPropertyName + " to value " + row.get(j) + ".");
				}
			}
		}
		an.setJsonProperties(jsonProperties.toString());
		return an;
	}

    /**
     * Transposes a Spark data frame by replacing its rows by its columns. All input columns are expected to be of type Double.
     * The fist column of the resulting data frame contains the column names of the input data frame and is of data type String. All other output columns are of type Double.
     * 
     * @param sc Current Spark context 
     * @param origDataFrame Data frame to be transposed
     * @return Transposed data frame
     */
	public static Dataset<Row> transposeDataFrame(SparkSession spark, Dataset<Row> origDataFrame) {
		Dataset<Row> transposedDataFrame = null;
		String[] origColumnNames = origDataFrame.columns();
		int origNumberColumns = origColumnNames.length;
		List<Row> origRows = origDataFrame.collectAsList();
		int origNumberRows = origRows.size();
		List<Row> transposedRows = new ArrayList<Row>();

		// Loop through columns of original DataFrame
		for (int i = 1; i < origNumberColumns; i++) {
			Object[] transposedRow = new Object[origNumberRows + 1];
			transposedRow[0] = origColumnNames[i];
			// Loop trough rows of original DataFrame
			for (int j = 0; j < origNumberRows; j++) {
				if (origRows.get(j).getString(i) == null) {
					transposedRow[j + 1] = null;
				} else {
					try {
						transposedRow[j + 1] = Double.parseDouble(origRows.get(j).getString(i));
					} catch(NumberFormatException e) {
						if (logger.getLevel() == Level.FINEST) {
							String msg = MessageFormat.format("Cannot convert DataFrame column {0} row {1} value ''{2}'' to Double.", new Object[] { i, j, origRows.get(j).getString(i) });
							logger.log(Level.FINEST, msg);
						}
						// Return null for all non-numeric fields
						transposedRow[j + 1] = null;
					}
				}
			}
			transposedRows.add(RowFactory.create(transposedRow));
		}

		// Store original column name in first column of transposed DataFrame
		StructField[] transposedColumnNames = new StructField[origNumberRows + 1];
		transposedColumnNames[0] = DataTypes.createStructField(origColumnNames[0], DataTypes.StringType, false);
		for (int j = 0; j < origNumberRows; j++) {
			transposedColumnNames[j + 1] = DataTypes.createStructField(origRows.get(j).getString(0), DataTypes.DoubleType, false);
		}
		StructType st = DataTypes.createStructType(transposedColumnNames);
		transposedDataFrame = spark.createDataFrame(transposedRows, st);
		return transposedDataFrame;
	}
}
