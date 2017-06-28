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
package org.apache.atlas.odf.api.connectivity;

import java.io.File;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.apache.atlas.odf.api.metadata.models.JDBCConnection;
import org.apache.atlas.odf.api.metadata.models.JDBCConnectionInfo;
import org.apache.atlas.odf.api.metadata.models.Column;
import org.apache.atlas.odf.api.metadata.models.DataFile;
import org.apache.atlas.odf.api.metadata.models.DataSet;
import org.apache.atlas.odf.api.metadata.models.RelationalDataSet;
import org.apache.atlas.odf.api.metadata.models.Table;
import org.apache.atlas.odf.api.discoveryservice.datasets.MaterializedDataSet;

/**
 * This class is a helper to retrieve actual data from a data source by passing an object that represents a reference to the dataset.
 *
 */
public class DataSetRetrieverImpl implements DataSetRetriever {

	Logger logger = Logger.getLogger(DataSetRetrieverImpl.class.getName());
	MetadataStore metaDataStore;

	public DataSetRetrieverImpl() {
	}

	public DataSetRetrieverImpl(MetadataStore metaDataStore) {
		this.metaDataStore = metaDataStore;
	}

	@Override
	public void setMetadataStore(MetadataStore mds) {
		this.metaDataStore = mds;
	}

	@Override
	public boolean canRetrieveDataSet(DataSet dataSet) {
		if (dataSet instanceof DataFile) {
			DataFile dataFile = (DataFile) dataSet;
			return getValidURL(dataFile) != null;
		} else if (dataSet instanceof Table) {
			Connection connection = getJDBCConnection((JDBCConnectionInfo) metaDataStore.getConnectionInfo(dataSet));
			if (connection != null) {
				try {
					connection.close();
					return true;
				} catch (SQLException e) {
					// do nothing
				}
			}
		}
		return false;
	}

	@Override
	public MaterializedDataSet retrieveRelationalDataSet(RelationalDataSet relationalDataSet) {
		if (relationalDataSet instanceof DataFile) {
			return retrieveDataFile((DataFile) relationalDataSet);
		} else if (relationalDataSet instanceof Table) {
			return retrieveTableWithJDBC((Table) relationalDataSet);
		}
		return null;
	}

	@Override
	public void createCsvFile(RelationalDataSet relationalDataSet, String fileName) {
		try {
			logger.log(Level.INFO, "Creating CSV input data file ", fileName);
			MaterializedDataSet mds = retrieveRelationalDataSet(relationalDataSet);
			PrintWriter printWriter = new PrintWriter(new File(fileName), "UTF-8") ;
			int columnCount = mds.getColumns().size();
			String headers = "\"" + mds.getColumns().get(0).getName() + "\"" ;
			for (int i = 1; i < columnCount; i++) {
				headers += ",\"" + mds.getColumns().get(i).getName() + "\"" ;
			}
			printWriter.println(headers);
			for (int i = 0; i < mds.getData().size(); i++) {
				String row = "\"" + mds.getData().get(i).get(0).toString() + "\"";
				for (int j = 1 ; j < columnCount; j++ ) {
					row += ",\"" + mds.getData().get(i).get(j).toString() + "\"";
				}
				printWriter.println(row);
			}
			printWriter.close();
		} catch(Exception exc) {
			throw new DataRetrievalException(exc);
		}
	}

	private URL getValidURL(DataFile dataFile) {
		try {
			Charset.forName(dataFile.getEncoding());
		} catch (Exception exc) {
			logger.log(Level.WARNING, MessageFormat.format("Encoding ''{0}'' of data file ''{1}''is not valid''", new Object[] { dataFile.getEncoding(), dataFile.getUrlString() }), exc);
			return null;
		}
		String urlString = dataFile.getUrlString();
		try {
			URL url = new URL(urlString);
			url.openConnection().connect();
			return url;
		} catch (Exception exc) {
			String msg = MessageFormat.format("Could not connect to data file URL ''{0}''. Error: {1}", new Object[] { urlString, exc.getMessage() });
			logger.log(Level.WARNING, msg, exc);
			return null;
		}
	}

	private MaterializedDataSet retrieveDataFile(DataFile dataFile) throws DataRetrievalException {
		URL url = this.getValidURL(dataFile);
		if (url == null) {
			return null;
		}
		List<Column> columns = metaDataStore.getColumns(dataFile);
		List<List<Object>> data = new ArrayList<>();

		try {
			CSVParser csvParser = CSVParser.parse(url, Charset.forName(dataFile.getEncoding()), CSVFormat.DEFAULT.withHeader());
			List<CSVRecord> records = csvParser.getRecords();
			Map<String, Integer> headerMap = csvParser.getHeaderMap();
			csvParser.close();

			for (CSVRecord record : records) {
				List<Object> targetRecord = new ArrayList<>();
				for (int i = 0; i < columns.size(); i++) {
					Column col = columns.get(i);
					String value = record.get(headerMap.get(col.getName()));
					Object convertedValue = value;
					if (col.getDataType().equals("int")) {
						convertedValue = Integer.parseInt(value);
					} else if (col.getDataType().equals("double")) {
						convertedValue = Double.parseDouble(value);
					}
					// TODO add more conversions
					targetRecord.add(convertedValue);
				}
				data.add(targetRecord);
			}

		} catch (Exception exc) {
			throw new DataRetrievalException(exc);
		}

		MaterializedDataSet materializedDS = new MaterializedDataSet();
		materializedDS.setTable(dataFile);
		materializedDS.setColumns(columns);
		materializedDS.setData(data);
		return materializedDS;
	}

	public static String quoteForJDBC(String s) {
		// TODO implement to prevent SQL injection
		return s;
	}

	Connection getJDBCConnection(JDBCConnectionInfo connectionInfo) {
		if ((connectionInfo.getConnections() == null) || connectionInfo.getConnections().isEmpty()) {
			return null;
		}
		JDBCConnection connectionObject = null;
		connectionObject = (JDBCConnection) connectionInfo.getConnections().get(0); // Use first connection
		try {
			return DriverManager.getConnection(connectionObject.getJdbcConnectionString(), connectionObject.getUser(), connectionObject.getPassword());
		} catch (SQLException exc) {
			logger.log(Level.WARNING, MessageFormat.format("JDBC connection to ''{0}'' for table ''{1}'' could not be created", new Object[] { connectionObject.getJdbcConnectionString(),
					connectionInfo.getSchemaName() + "." + connectionInfo.getTableName() }), exc);
			return null;
		}
	}

	private MaterializedDataSet retrieveTableWithJDBC(Table table) {

		JDBCRetrievalResult jdbcRetrievalResult = this.retrieveTableAsJDBCResultSet(table);
		if (jdbcRetrievalResult == null) {
			logger.log(Level.FINE, "JDBC retrieval result for table ''{0}'' is null", table.getReference().getUrl());
			return null;
		}

		Map<String, Column> columnMap = new HashMap<String, Column>();
		for (Column column : metaDataStore.getColumns(table)) {
			columnMap.put(column.getName(), column);
		}
		logger.log(Level.INFO, "Table columns: {0}", columnMap.keySet());

		ResultSet rs = null;
		try {
			logger.log(Level.FINE, "Executing prepared statement " + jdbcRetrievalResult.getPreparedStatement());
			rs = jdbcRetrievalResult.getPreparedStatement().executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			List<Column> resultSetColumns = new ArrayList<>();
			int columnCount = rsmd.getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				Column col = new Column();
				col.setName(rsmd.getColumnName(i));
				col.setDataType(rsmd.getColumnTypeName(i));
				Column retrievedColumn = columnMap.get(col.getName());
				if (retrievedColumn != null && retrievedColumn.getReference() != null) {
					col.setReference(retrievedColumn.getReference());
				} else {
					logger.log(Level.WARNING, "Error setting reference on column, this can cause issues when annotations are created on the column!");
				}
				resultSetColumns.add(col);
			}

			List<List<Object>> data = new ArrayList<>();
			while (rs.next()) {
				List<Object> row = new ArrayList<>();
				for (int i = 1; i <= columnCount; i++) {
					row.add(rs.getObject(i));
				}
				data.add(row);
			}

			MaterializedDataSet result = new MaterializedDataSet();
			result.setTable(table);
			result.setColumns(resultSetColumns);
			result.setData(data);
			return result;
		} catch (SQLException exc) {
			throw new DataRetrievalException(exc);
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				jdbcRetrievalResult.close();
			} catch (SQLException exc) {
				throw new DataRetrievalException(exc);
			}
		}

	}

	@Override
	public JDBCRetrievalResult retrieveTableAsJDBCResultSet(Table table) {
		JDBCConnectionInfo connectionInfo = (JDBCConnectionInfo) this.metaDataStore.getConnectionInfo(table);
		Connection connection = null;
		PreparedStatement stat = null;
		try {
			connection = this.getJDBCConnection(connectionInfo);
			if (connection == null) {
				logger.log(Level.FINE, "No jdbc connection found for table ''{0}'' (''{1}'')", new Object[]{table.getName(), table.getReference().getUrl()});
				return null;
			}
			String schemaName = connectionInfo.getSchemaName();
			String sql = "select * from " + quoteForJDBC(schemaName) + "." + quoteForJDBC(table.getName());
			logger.log(Level.FINER, "Running JDBC statement: ''{0}''", sql);
			stat = connection.prepareStatement(sql);
			return new JDBCRetrievalResult(connection, stat);
		} catch (SQLException exc) {
			String msg = MessageFormat.format("An SQL exception occurred when preparing data access for table ''{0}'' ({1})", new Object[]{table.getName(), table.getReference().getUrl()});
			logger.log(Level.WARNING, msg, exc);
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException exc2) {
				// do nothing
				logger.log(Level.WARNING, msg, exc2);
				throw new DataRetrievalException(exc2);
			}
			throw new DataRetrievalException(exc);
		}
	}
}
