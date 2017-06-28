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
package org.apache.atlas.odf.core.metadata;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.MetadataStoreException;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.metadata.models.ClassificationAnnotation;
import org.apache.atlas.odf.api.metadata.models.JDBCConnection;
import org.apache.atlas.odf.api.metadata.models.JDBCConnectionInfo;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.api.metadata.models.BusinessTerm;
import org.apache.atlas.odf.api.metadata.models.Column;
import org.apache.atlas.odf.api.metadata.models.Connection;
import org.apache.atlas.odf.api.metadata.models.ConnectionInfo;
import org.apache.atlas.odf.api.metadata.models.DataFile;
import org.apache.atlas.odf.api.metadata.models.DataFileFolder;
import org.apache.atlas.odf.api.metadata.models.DataSet;
import org.apache.atlas.odf.api.metadata.models.DataStore;
import org.apache.atlas.odf.api.metadata.models.Database;
import org.apache.atlas.odf.api.metadata.models.Document;
import org.apache.atlas.odf.api.metadata.models.RelationalDataSet;
import org.apache.atlas.odf.api.metadata.models.Schema;
import org.apache.atlas.odf.api.metadata.models.Table;
import org.apache.atlas.odf.api.metadata.models.UnknownDataSet;
import org.apache.atlas.odf.api.metadata.models.ProfilingAnnotation;
import org.apache.atlas.odf.api.metadata.models.RelationshipAnnotation;

/**
 * Utilities to be used for implementing the {@link WritableMetadataStore} interface, i.e. for
 * adding support for an additional writable metadata store to ODF.
 *
 *
 */
public class WritableMetadataStoreUtils {

	/**
	 * Utility method for creating an populating a new {@link Column} object. The object will have a generated reference
	 * that uses a random id and points to a given metadata store.
	 *
	 * @param mds Metadata store to which the reference of the new column should point.
	 * @param name Name of the new column
	 * @param dataType Data type of the new column
	 * @param description Description of the new column
	 * @return The resulting column object
	 */
	public static Column createColumn(String name, String dataType, String description) {
		Column column = new Column();
		column.setName(name);
		column.setDescription(description);
		column.setDataType(dataType);
		return column;
	}

	public static String getFileUrl(String shortFileName) {
		if (System.getProperty("os.name").toLowerCase().startsWith("windows")) {
			return "file://localhost/c:/tmp/" + shortFileName;
		} else {
			return "file:///tmp/" + shortFileName;
		}
	}

	/**
	 * Utility method for genrating a new metadata object reference that uses a random id and points
	 * to a given metadata store.
	 *
	 * @param mds Metadata store to which the new reference should point
	 * @return The resulting metadata object reference
	 */
	public static MetaDataObjectReference generateMdoRef(MetadataStore mds) {
		MetaDataObjectReference ref = new MetaDataObjectReference();
		ref.setId(UUID.randomUUID().toString());
		ref.setRepositoryId(mds.getRepositoryId());
		ref.setUrl("");
		return ref;
	}

	/**
	 * Utility method providing the list of ODF example objects used for the ODF integration tests.
	 * The references of the example objects point to a given metadata store.
	 *
	 * @param mds Metadata store
	 * @return List of example objects
	 */
	public static void createSampleDataObjects(WritableMetadataStore mds) {
		DataFile bankClients = new DataFile();
		bankClients.setName("BankClientsShort");
		bankClients.setDescription("A reduced sample data file containing bank clients.");
		bankClients.setUrlString(getFileUrl("bank-clients-short.csv"));
		mds.createObject(bankClients);
		mds.addColumnReference(bankClients, mds.createObject(createColumn("CLIENT_ID", "string", "A client ID (column 1)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("NAME", "string", "A client name (column 2)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("ADDRESS", "string", "A client's address (column 3)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("ZIP", "string", "Zip code (column 4)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("AGE", "double", "Age in years (column 5)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("GENDER", "string", "Person gender (column 6)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("MARITAL_STATUS", "string", "Marital status (column 7)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("PROFESSION", "string", "Profession (column 8)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("NBR_YEARS_CLI", "double", "The number of years how long the client has been with us (column 9)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("SAVINGS_ACCOUNT", "string", "Savings account number (column 10)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("ONLINE_ACCESS", "string", "A flag indicating if the client accesses her accounts online (column 11)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("JOINED_ACCOUNTS", "string", "A flag indicating if the client has joined accounts (column 12)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("BANKCARD", "string", "A flag indicating if the client has a bankcard (column 13)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("AVERAGE_BALANCE", "double", "The average balance over the last year (column 14)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("ACCOUNT_ID", "int", "Account Id / number (column 15)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("ACCOUNT_TYPE", "string", "Type of account (column 16)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("EMAIL", "string", "A flag indicating if the client has joined accounts (column 17)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("CCN", "string", "Credit card number (column 18)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("PHONE1", "string", "Primary hone number (column 19)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("PHONE2", "string", "Secondary phone number (column 20)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("CC", "string", "CC indicator (column 21)")));
		mds.addColumnReference(bankClients, mds.createObject(createColumn("CONTACT", "string", "Contact in case of emergency (column 22)")));

		DataFile simpleExampleTable = new DataFile();
		simpleExampleTable.setName("SimpleExampleTable");
		simpleExampleTable.setDescription("A very simple example document referring to a local file.");
		simpleExampleTable.setUrlString(getFileUrl("simple-example-table.csv"));
		mds.createObject(simpleExampleTable);
		mds.addColumnReference(simpleExampleTable, mds.createObject(createColumn("ColumnName1", "string", null)));
		mds.addColumnReference(simpleExampleTable, mds.createObject(createColumn("ColumnName2", "int", null)));

		Document simpleExampleURLDocument = new Document();
		simpleExampleURLDocument.setName("Simple URL example document");
		simpleExampleURLDocument.setDescription("A very simple example document referring to a publicly available URL");
		simpleExampleURLDocument.setUrlString("https://www.wikipedia.org");
		simpleExampleURLDocument.setEncoding("ASCII");
		mds.createObject(simpleExampleURLDocument);

		Document simpleExampleDocument = new Document();
		simpleExampleDocument.setName("Simple local example document");
		simpleExampleDocument.setDescription("A very simple example document referring to a local file");
		simpleExampleDocument.setUrlString(getFileUrl("simple-example-document.txt"));
		simpleExampleDocument.setEncoding("ASCII");
		mds.createObject(simpleExampleDocument);

		BusinessTerm bankClientTerm1 = new BusinessTerm();
		bankClientTerm1.setName("Address");
		bankClientTerm1.setDescription("The mail address of a person or organization");
		bankClientTerm1.setAbbreviations(Arrays.asList(new String[] { "Addr" }));
		bankClientTerm1.setExample("257 Great Lister Street P O BOX 1107 Birmingham");
		bankClientTerm1.setUsage("Outgoing mail (physical).");
		mds.createObject(bankClientTerm1);

		BusinessTerm bankClientTerm2a = new BusinessTerm();
		bankClientTerm2a.setName("Marital Status");
		bankClientTerm2a.setDescription("The marital status of a person (single, married, divorced, or other).");
		bankClientTerm2a.setAbbreviations(Arrays.asList(new String[] { "MS","MAST" }));
		bankClientTerm2a.setExample("single");
		bankClientTerm2a.setUsage("Contracting");
		mds.createObject(bankClientTerm2a);

		BusinessTerm bankClientTerm2b = new BusinessTerm();
		bankClientTerm2b.setReference(generateMdoRef(mds));
		bankClientTerm2b.setName("Marital Status");
		bankClientTerm2b.setDescription("2nd term representing the marital status of a person.");
		bankClientTerm2b.setAbbreviations(Arrays.asList(new String[] { "MS","MAST" }));
		bankClientTerm2b.setExample("married");
		bankClientTerm2b.setUsage("Human Resources");
		mds.createObject(bankClientTerm2b);

		BusinessTerm bankClientTerm3 = new BusinessTerm();
		bankClientTerm3.setName("AVG Balance");
		bankClientTerm3.setDescription("The average balance of an account over an amount of time, typically a year. Unit: Dollars.");
		bankClientTerm3.setAbbreviations(Arrays.asList(new String[] { "AB","AVGB","AVGBAL" }));
		bankClientTerm3.setExample("1000");
		bankClientTerm3.setUsage("Contracting");
		bankClientTerm3.setOriginRef("test-pointer-to-igc");
		bankClientTerm3.setReplicaRefs(Arrays.asList(new String[] { "first-replica-pointer", "second-replica-pointer" }));
		mds.createObject(bankClientTerm3);

		BusinessTerm bankClientTerm4 = new BusinessTerm();
		bankClientTerm4.setName("LASTNAME");
		bankClientTerm4.setDescription("Last name of a person");
		bankClientTerm4.setAbbreviations(Arrays.asList(new String[] { "LASTNME" }));
		bankClientTerm4.setExample("1000");
		bankClientTerm4.setUsage("Contracting");
		mds.createObject(bankClientTerm4);

		BusinessTerm bankClientTerm5a = new BusinessTerm();
		bankClientTerm5a.setReference(generateMdoRef(mds));
		bankClientTerm5a.setName("Credit Card Number");
		bankClientTerm5a.setDescription("Credit card number of a customer");
		bankClientTerm5a.setAbbreviations(Arrays.asList(new String[] { "CreNum", "CCN" }));
		bankClientTerm5a.setExample("1234567");
		bankClientTerm5a.setUsage("Contracting");
		mds.createObject(bankClientTerm5a);

		BusinessTerm bankClientTerm5b = new BusinessTerm();
		bankClientTerm5b.setReference(generateMdoRef(mds));
		bankClientTerm5b.setName("Credit Card Number");
		bankClientTerm5b.setDescription("Credit card number of an employee");
		bankClientTerm5b.setAbbreviations(Arrays.asList(new String[] {}));      // this one has no abbreviations
		bankClientTerm5b.setExample("1234567");
		bankClientTerm5b.setUsage("Human Resources");
		mds.createObject(bankClientTerm5b);

		BusinessTerm bankClientTermDataSetLevel = new BusinessTerm();
		bankClientTermDataSetLevel.setName("Bank Clients");
		bankClientTermDataSetLevel.setDescription("The only purpose of this term is to match the name of the data set BankClientsShort");
		bankClientTermDataSetLevel.setAbbreviations(Arrays.asList(new String[] { "BC" }));
		bankClientTermDataSetLevel.setExample("<none>");
		bankClientTermDataSetLevel.setUsage("Integration testing of TermMatcher discovery service. Yields confidence value of 56.");
		mds.createObject(bankClientTermDataSetLevel);

		mds.commit();
	}

	/**
	 * Utility method that returns the list of ODF base types that need to be supported by a metadata store in order to be used with ODF.
	 *
	 * @return List of the ODF base types
	 */
	public static final List<Class<?>> getBaseTypes() {
		List<Class<?>> typeList = new ArrayList<Class<?>>();
		typeList.add(MetaDataObject.class);
		typeList.add(DataStore.class);
		typeList.add(Database.class);
		typeList.add(Connection.class);
		typeList.add(JDBCConnection.class);
		typeList.add(DataSet.class);
		typeList.add(UnknownDataSet.class);
		typeList.add(RelationalDataSet.class);
		typeList.add(Column.class);
		typeList.add(Table.class);
		typeList.add(Schema.class);
		typeList.add(DataFileFolder.class);
		typeList.add(DataFile.class);
		typeList.add(Document.class);
		typeList.add(Annotation.class);
		typeList.add(ProfilingAnnotation.class);
		typeList.add(ClassificationAnnotation.class);
		typeList.add(RelationshipAnnotation.class);
		typeList.add(BusinessTerm.class);
		return typeList;
	}

	/**
	* Utility method that returns a connection info object for a given information asset.
	*
	* @return Connection info object
	*/
    public static ConnectionInfo getConnectionInfo(MetadataStore mds, MetaDataObject informationAsset) {
		if (informationAsset instanceof Table) {
			Schema schema = getParentOfType(mds, informationAsset, Schema.class);
			Database database = getParentOfType(mds, schema, Database.class);
			JDBCConnectionInfo jdbcConnectionInfo = new JDBCConnectionInfo();
			jdbcConnectionInfo.setSchemaName(schema.getName());
			jdbcConnectionInfo.setTableName(informationAsset.getName());
			jdbcConnectionInfo.setConnections(mds.getConnections(database));
			jdbcConnectionInfo.setAssetReference(informationAsset.getReference());
            return jdbcConnectionInfo;
        }
		return null;
    };

    /**
	 * Utility to return the parent of a metadata object casted to a given type.
	 * An exception is thrown if the types don't match.
	 *
	 * @param mds Metadata store
	 * @param metaDataObject Metadata object
	 * @param type Class to which the parent should be casted
	 * @return Parent object of the given metadata object
	 */
	public static <T> T getParentOfType(MetadataStore mds, MetaDataObject metaDataObject, Class<T> type) {
		MetaDataObject parent = mds.getParent(metaDataObject);
		if (parent == null) {
			String errorMessage = MessageFormat.format("Cannot extract connection info for object id ''{0}'' because the parent object is null.", metaDataObject.getReference().getId());
			throw new MetadataStoreException(errorMessage);
		}
		if (!type.isInstance(parent)) {
			String errorMessage = MessageFormat.format("Parent of object ''{0}'' is expected to be of type ''{1}'' but is ''{2}''",
					new Object[] { metaDataObject.getReference().getId(), type.getSimpleName(), parent.getClass().getName() });
	        throw new MetadataStoreException(errorMessage);
		}
		return type.cast(parent);
	}

}
