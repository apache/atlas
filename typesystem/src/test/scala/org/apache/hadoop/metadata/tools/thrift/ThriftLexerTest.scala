package org.apache.hadoop.metadata.tools.thrift

import org.junit.Test
import org.junit.Assert

import scala.util.parsing.input.CharArrayReader

class ThriftLexerTest {

  def scan(p : ThriftParser, str : String) : p.lexical.ParseResult[_] = {
    val l = p.lexical
    var s: l.Input = new CharArrayReader(str.toCharArray)
    var r = (l.whitespace.? ~ l.token)(s)
    s = r.next

    while (r.successful && !s.atEnd) {
      s = r.next
      if ( !s.atEnd ) {
        r = (l.whitespace.? ~ l.token)(s)
      }
    }
    r.asInstanceOf[p.lexical.ParseResult[_]]
  }

  @Test def testSimple {
    val p = new ThriftParser
    val r = scan(p, """efg abc""")
    Assert.assertTrue(r.successful)

  }

  @Test def testStruct {
    val p = new ThriftParser
    val r = scan(p, """struct PartitionSpecWithSharedSD {
           1: list<PartitionWithoutSD> partitions,
           2: StorageDescriptor sd,
         }""")
    Assert.assertTrue(r.successful)

  }

  @Test def testTableStruct {
    val p = new ThriftParser
    val r = scan(p, """// table information
         struct Table {
           1: string tableName,                // name of the table
           2: string dbName,                   // database name ('default')
           3: string owner,                    // owner of this table
           4: i32    createTime,               // creation time of the table
           5: i32    lastAccessTime,           // last access time (usually this will be filled from HDFS and shouldn't be relied on)
           6: i32    retention,                // retention time
           7: StorageDescriptor sd,            // storage descriptor of the table
           8: list<FieldSchema> partitionKeys, // partition keys of the table. only primitive types are supported
           9: map<string, string> parameters,   // to store comments or any other user level parameters
           10: string viewOriginalText,         // original view text, null for non-view
           11: string viewExpandedText,         // expanded view text, null for non-view
           12: string tableType,                 // table type enum, e.g. EXTERNAL_TABLE
           13: optional PrincipalPrivilegeSet privileges,
           14: optional bool temporary=false
         }""")
    Assert.assertTrue(r.successful)

  }

  @Test def testIncorrectStruct {
    val p = new ThriftParser
    val r = scan(p, """// table information
         struct Table {
        | 1: string tableName,                // name of the table
        | 2: string dbName
          }""")
    Assert.assertFalse(r.successful)

  }

  @Test def testService {
    val p = new ThriftParser
    val r = scan(p, """/**
             * This interface is live.
             */
             service ThriftHiveMetastore extends fb303.FacebookService
             {
               string getMetaConf(1:string key) throws(1:MetaException o1)
               void setMetaConf(1:string key, 2:string value) throws(1:MetaException o1)

               void create_database(1:Database database) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3)
               Database get_database(1:string name) throws(1:NoSuchObjectException o1, 2:MetaException o2)
               void drop_database(1:string name, 2:bool deleteData, 3:bool cascade) throws(1:NoSuchObjectException o1, 2:InvalidOperationException o2, 3:MetaException o3)
               list<string> get_databases(1:string pattern) throws(1:MetaException o1)
               list<string> get_all_databases() throws(1:MetaException o1)
               void alter_database(1:string dbname, 2:Database db) throws(1:MetaException o1, 2:NoSuchObjectException o2)

               // returns the type with given name (make seperate calls for the dependent types if needed)
               Type get_type(1:string name)  throws(1:MetaException o1, 2:NoSuchObjectException o2)
               bool create_type(1:Type type) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3)
               bool drop_type(1:string type) throws(1:MetaException o1, 2:NoSuchObjectException o2)
               map<string, Type> get_type_all(1:string name)
                                             throws(1:MetaException o2)

               // Gets a list of FieldSchemas describing the columns of a particular table
               list<FieldSchema> get_fields(1: string db_name, 2: string table_name) throws (1: MetaException o1, 2: UnknownTableException o2, 3: UnknownDBException o3),

               // Gets a list of FieldSchemas describing both the columns and the partition keys of a particular table
               list<FieldSchema> get_schema(1: string db_name, 2: string table_name) throws (1: MetaException o1, 2: UnknownTableException o2, 3: UnknownDBException o3)

               // create a Hive table. Following fields must be set
               // tableName
               // database        (only 'default' for now until Hive QL supports databases)
               // owner           (not needed, but good to have for tracking purposes)
               // sd.cols         (list of field schemas)
               // sd.inputFormat  (SequenceFileInputFormat (binary like falcon tables or u_full) or TextInputFormat)
               // sd.outputFormat (SequenceFileInputFormat (binary) or TextInputFormat)
               // sd.serdeInfo.serializationLib (SerDe class name eg org.apache.hadoop.hive.serde.simple_meta.MetadataTypedColumnsetSerDe
               // * See notes on DDL_TIME
               void create_table(1:Table tbl) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3, 4:NoSuchObjectException o4)
               void create_table_with_environment_context(1:Table tbl,
                   2:EnvironmentContext environment_context)
                   throws (1:AlreadyExistsException o1,
                           2:InvalidObjectException o2, 3:MetaException o3,
                           4:NoSuchObjectException o4)
               // drops the table and all the partitions associated with it if the table has partitions
               // delete data (including partitions) if deleteData is set to true
               void drop_table(1:string dbname, 2:string name, 3:bool deleteData)
                                    throws(1:NoSuchObjectException o1, 2:MetaException o3)
               void drop_table_with_environment_context(1:string dbname, 2:string name, 3:bool deleteData,
                   4:EnvironmentContext environment_context)
                                    throws(1:NoSuchObjectException o1, 2:MetaException o3)
               list<string> get_tables(1: string db_name, 2: string pattern) throws (1: MetaException o1)
               list<string> get_all_tables(1: string db_name) throws (1: MetaException o1)

               Table get_table(1:string dbname, 2:string tbl_name)
                                    throws (1:MetaException o1, 2:NoSuchObjectException o2)
               list<Table> get_table_objects_by_name(1:string dbname, 2:list<string> tbl_names)
             				   throws (1:MetaException o1, 2:InvalidOperationException o2, 3:UnknownDBException o3)

               // Get a list of table names that match a filter.
               // The filter operators are LIKE, <, <=, >, >=, =, <>
               //
               // In the filter statement, values interpreted as strings must be enclosed in quotes,
               // while values interpreted as integers should not be.  Strings and integers are the only
               // supported value types.
               //
               // The currently supported key names in the filter are:
               // Constants.HIVE_FILTER_FIELD_OWNER, which filters on the tables' owner's name
               //   and supports all filter operators
               // Constants.HIVE_FILTER_FIELD_LAST_ACCESS, which filters on the last access times
               //   and supports all filter operators except LIKE
               // Constants.HIVE_FILTER_FIELD_PARAMS, which filters on the tables' parameter keys and values
               //   and only supports the filter operators = and <>.
               //   Append the parameter key name to HIVE_FILTER_FIELD_PARAMS in the filter statement.
               //   For example, to filter on parameter keys called "retention", the key name in the filter
               //   statement should be Constants.HIVE_FILTER_FIELD_PARAMS + "retention"
               //   Also, = and <> only work for keys that exist
               //   in the tables. E.g., if you are looking for tables where key1 <> value, it will only
               //   look at tables that have a value for the parameter key1.
               // Some example filter statements include:
               // filter = Constants.HIVE_FILTER_FIELD_OWNER + " like \".*test.*\" and " +
               //   Constants.HIVE_FILTER_FIELD_LAST_ACCESS + " = 0";
               // filter = Constants.HIVE_FILTER_FIELD_PARAMS + "retention = \"30\" or " +
               //   Constants.HIVE_FILTER_FIELD_PARAMS + "retention = \"90\""
               // @param dbName
               //          The name of the database from which you will retrieve the table names
               // @param filterType
               //          The type of filter
               // @param filter
               //          The filter string
               // @param max_tables
               //          The maximum number of tables returned
               // @return  A list of table names that match the desired filter
               list<string> get_table_names_by_filter(1:string dbname, 2:string filter, 3:i16 max_tables=-1)
                                    throws (1:MetaException o1, 2:InvalidOperationException o2, 3:UnknownDBException o3)

               // alter table applies to only future partitions not for existing partitions
               // * See notes on DDL_TIME
               void alter_table(1:string dbname, 2:string tbl_name, 3:Table new_tbl)
                                    throws (1:InvalidOperationException o1, 2:MetaException o2)
               void alter_table_with_environment_context(1:string dbname, 2:string tbl_name,
                   3:Table new_tbl, 4:EnvironmentContext environment_context)
                   throws (1:InvalidOperationException o1, 2:MetaException o2)
               // the following applies to only tables that have partitions
               // * See notes on DDL_TIME
               Partition add_partition(1:Partition new_part)
                                    throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
               Partition add_partition_with_environment_context(1:Partition new_part,
                   2:EnvironmentContext environment_context)
                   throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2,
                   3:MetaException o3)
               i32 add_partitions(1:list<Partition> new_parts)
                                    throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
               i32 add_partitions_pspec(1:list<PartitionSpec> new_parts)
                                    throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
               Partition append_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals)
                                    throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
               AddPartitionsResult add_partitions_req(1:AddPartitionsRequest request)
                                    throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
               Partition append_partition_with_environment_context(1:string db_name, 2:string tbl_name,
                   3:list<string> part_vals, 4:EnvironmentContext environment_context)
                                    throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
               Partition append_partition_by_name(1:string db_name, 2:string tbl_name, 3:string part_name)
                                    throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
               Partition append_partition_by_name_with_environment_context(1:string db_name, 2:string tbl_name,
                   3:string part_name, 4:EnvironmentContext environment_context)
                                    throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
               bool drop_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals, 4:bool deleteData)
                                    throws(1:NoSuchObjectException o1, 2:MetaException o2)
               bool drop_partition_with_environment_context(1:string db_name, 2:string tbl_name,
                   3:list<string> part_vals, 4:bool deleteData, 5:EnvironmentContext environment_context)
                                    throws(1:NoSuchObjectException o1, 2:MetaException o2)
               bool drop_partition_by_name(1:string db_name, 2:string tbl_name, 3:string part_name, 4:bool deleteData)
                                    throws(1:NoSuchObjectException o1, 2:MetaException o2)
               bool drop_partition_by_name_with_environment_context(1:string db_name, 2:string tbl_name,
                   3:string part_name, 4:bool deleteData, 5:EnvironmentContext environment_context)
                                    throws(1:NoSuchObjectException o1, 2:MetaException o2)
               DropPartitionsResult drop_partitions_req(1: DropPartitionsRequest req)
                                    throws(1:NoSuchObjectException o1, 2:MetaException o2)

               Partition get_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals)
                                    throws(1:MetaException o1, 2:NoSuchObjectException o2)
               Partition exchange_partition(1:map<string, string> partitionSpecs, 2:string source_db,
                   3:string source_table_name, 4:string dest_db, 5:string dest_table_name)
                   throws(1:MetaException o1, 2:NoSuchObjectException o2, 3:InvalidObjectException o3,
                   4:InvalidInputException o4)

               Partition get_partition_with_auth(1:string db_name, 2:string tbl_name, 3:list<string> part_vals,
                   4: string user_name, 5: list<string> group_names) throws(1:MetaException o1, 2:NoSuchObjectException o2)

               Partition get_partition_by_name(1:string db_name 2:string tbl_name, 3:string part_name)
                                    throws(1:MetaException o1, 2:NoSuchObjectException o2)

               // returns all the partitions for this table in reverse chronological order.
               // If max parts is given then it will return only that many.
               list<Partition> get_partitions(1:string db_name, 2:string tbl_name, 3:i16 max_parts=-1)
                                    throws(1:NoSuchObjectException o1, 2:MetaException o2)
               list<Partition> get_partitions_with_auth(1:string db_name, 2:string tbl_name, 3:i16 max_parts=-1,
                  4: string user_name, 5: list<string> group_names) throws(1:NoSuchObjectException o1, 2:MetaException o2)

               list<PartitionSpec> get_partitions_pspec(1:string db_name, 2:string tbl_name, 3:i32 max_parts=-1)
                                    throws(1:NoSuchObjectException o1, 2:MetaException o2)

               list<string> get_partition_names(1:string db_name, 2:string tbl_name, 3:i16 max_parts=-1)
                                    throws(1:MetaException o2)

               // get_partition*_ps methods allow filtering by a partial partition specification,
               // as needed for dynamic partitions. The values that are not restricted should
               // be empty strings. Nulls were considered (instead of "") but caused errors in
               // generated Python code. The size of part_vals may be smaller than the
               // number of partition columns - the unspecified values are considered the same
               // as "".
               list<Partition> get_partitions_ps(1:string db_name 2:string tbl_name
               	3:list<string> part_vals, 4:i16 max_parts=-1)
                                    throws(1:MetaException o1, 2:NoSuchObjectException o2)
               list<Partition> get_partitions_ps_with_auth(1:string db_name, 2:string tbl_name, 3:list<string> part_vals, 4:i16 max_parts=-1,
                  5: string user_name, 6: list<string> group_names) throws(1:NoSuchObjectException o1, 2:MetaException o2)

               list<string> get_partition_names_ps(1:string db_name,
               	2:string tbl_name, 3:list<string> part_vals, 4:i16 max_parts=-1)
               	                   throws(1:MetaException o1, 2:NoSuchObjectException o2)

               // get the partitions matching the given partition filter
               list<Partition> get_partitions_by_filter(1:string db_name 2:string tbl_name
                 3:string filter, 4:i16 max_parts=-1)
                                    throws(1:MetaException o1, 2:NoSuchObjectException o2)

               // List partitions as PartitionSpec instances.
               list<PartitionSpec> get_part_specs_by_filter(1:string db_name 2:string tbl_name
                 3:string filter, 4:i32 max_parts=-1)
                                    throws(1:MetaException o1, 2:NoSuchObjectException o2)

               // get the partitions matching the given partition filter
               // unlike get_partitions_by_filter, takes serialized hive expression, and with that can work
               // with any filter (get_partitions_by_filter only works if the filter can be pushed down to JDOQL.
               PartitionsByExprResult get_partitions_by_expr(1:PartitionsByExprRequest req)
                                    throws(1:MetaException o1, 2:NoSuchObjectException o2)

               // get partitions give a list of partition names
               list<Partition> get_partitions_by_names(1:string db_name 2:string tbl_name 3:list<string> names)
                                    throws(1:MetaException o1, 2:NoSuchObjectException o2)

               // changes the partition to the new partition object. partition is identified from the part values
               // in the new_part
               // * See notes on DDL_TIME
               void alter_partition(1:string db_name, 2:string tbl_name, 3:Partition new_part)
                                    throws (1:InvalidOperationException o1, 2:MetaException o2)

               // change a list of partitions. All partitions are altered atomically and all
               // prehooks are fired together followed by all post hooks
               void alter_partitions(1:string db_name, 2:string tbl_name, 3:list<Partition> new_parts)
                                    throws (1:InvalidOperationException o1, 2:MetaException o2)

               void alter_partition_with_environment_context(1:string db_name,
                   2:string tbl_name, 3:Partition new_part,
                   4:EnvironmentContext environment_context)
                   throws (1:InvalidOperationException o1, 2:MetaException o2)

               // rename the old partition to the new partition object by changing old part values to the part values
               // in the new_part. old partition is identified from part_vals.
               // partition keys in new_part should be the same as those in old partition.
               void rename_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals, 4:Partition new_part)
                                    throws (1:InvalidOperationException o1, 2:MetaException o2)

               // returns whether or not the partition name is valid based on the value of the config
               // hive.metastore.partition.name.whitelist.pattern
               bool partition_name_has_valid_characters(1:list<string> part_vals, 2:bool throw_exception)
              	throws(1: MetaException o1)

               // gets the value of the configuration key in the metastore server. returns
               // defaultValue if the key does not exist. if the configuration key does not
               // begin with "hive", "mapred", or "hdfs", a ConfigValSecurityException is
               // thrown.
               string get_config_value(1:string name, 2:string defaultValue)
                                       throws(1:ConfigValSecurityException o1)

               // converts a partition name into a partition values array
               list<string> partition_name_to_vals(1: string part_name)
                                       throws(1: MetaException o1)
               // converts a partition name into a partition specification (a mapping from
               // the partition cols to the values)
               map<string, string> partition_name_to_spec(1: string part_name)
                                       throws(1: MetaException o1)

               void markPartitionForEvent(1:string db_name, 2:string tbl_name, 3:map<string,string> part_vals,
                               4:PartitionEventType eventType) throws (1: MetaException o1, 2: NoSuchObjectException o2,
                               3: UnknownDBException o3, 4: UnknownTableException o4, 5: UnknownPartitionException o5,
                               6: InvalidPartitionException o6)
               bool isPartitionMarkedForEvent(1:string db_name, 2:string tbl_name, 3:map<string,string> part_vals,
                               4: PartitionEventType eventType) throws (1: MetaException o1, 2:NoSuchObjectException o2,
                               3: UnknownDBException o3, 4: UnknownTableException o4, 5: UnknownPartitionException o5,
                               6: InvalidPartitionException o6)

               //index
               Index add_index(1:Index new_index, 2: Table index_table)
                                    throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
               void alter_index(1:string dbname, 2:string base_tbl_name, 3:string idx_name, 4:Index new_idx)
                                    throws (1:InvalidOperationException o1, 2:MetaException o2)
               bool drop_index_by_name(1:string db_name, 2:string tbl_name, 3:string index_name, 4:bool deleteData)
                                    throws(1:NoSuchObjectException o1, 2:MetaException o2)
               Index get_index_by_name(1:string db_name 2:string tbl_name, 3:string index_name)
                                    throws(1:MetaException o1, 2:NoSuchObjectException o2)

               list<Index> get_indexes(1:string db_name, 2:string tbl_name, 3:i16 max_indexes=-1)
                                    throws(1:NoSuchObjectException o1, 2:MetaException o2)
               list<string> get_index_names(1:string db_name, 2:string tbl_name, 3:i16 max_indexes=-1)
                                    throws(1:MetaException o2)

               // column statistics interfaces

               // update APIs persist the column statistics object(s) that are passed in. If statistics already
               // exists for one or more columns, the existing statistics will be overwritten. The update APIs
               // validate that the dbName, tableName, partName, colName[] passed in as part of the ColumnStatistics
               // struct are valid, throws InvalidInputException/NoSuchObjectException if found to be invalid
               bool update_table_column_statistics(1:ColumnStatistics stats_obj) throws (1:NoSuchObjectException o1,
                           2:InvalidObjectException o2, 3:MetaException o3, 4:InvalidInputException o4)
               bool update_partition_column_statistics(1:ColumnStatistics stats_obj) throws (1:NoSuchObjectException o1,
                           2:InvalidObjectException o2, 3:MetaException o3, 4:InvalidInputException o4)

               // get APIs return the column statistics corresponding to db_name, tbl_name, [part_name], col_name if
               // such statistics exists. If the required statistics doesn't exist, get APIs throw NoSuchObjectException
               // For instance, if get_table_column_statistics is called on a partitioned table for which only
               // partition level column stats exist, get_table_column_statistics will throw NoSuchObjectException
               ColumnStatistics get_table_column_statistics(1:string db_name, 2:string tbl_name, 3:string col_name) throws
                           (1:NoSuchObjectException o1, 2:MetaException o2, 3:InvalidInputException o3, 4:InvalidObjectException o4)
               ColumnStatistics get_partition_column_statistics(1:string db_name, 2:string tbl_name, 3:string part_name,
                            4:string col_name) throws (1:NoSuchObjectException o1, 2:MetaException o2,
                            3:InvalidInputException o3, 4:InvalidObjectException o4)
               TableStatsResult get_table_statistics_req(1:TableStatsRequest request) throws
                           (1:NoSuchObjectException o1, 2:MetaException o2)
               PartitionsStatsResult get_partitions_statistics_req(1:PartitionsStatsRequest request) throws
                           (1:NoSuchObjectException o1, 2:MetaException o2)
               AggrStats get_aggr_stats_for(1:PartitionsStatsRequest request) throws
                           (1:NoSuchObjectException o1, 2:MetaException o2)
               bool set_aggr_stats_for(1:SetPartitionsStatsRequest request) throws
                           (1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3, 4:InvalidInputException o4)


               // delete APIs attempt to delete column statistics, if found, associated with a given db_name, tbl_name, [part_name]
               // and col_name. If the delete API doesn't find the statistics record in the metastore, throws NoSuchObjectException
               // Delete API validates the input and if the input is invalid throws InvalidInputException/InvalidObjectException.
               bool delete_partition_column_statistics(1:string db_name, 2:string tbl_name, 3:string part_name, 4:string col_name) throws
                           (1:NoSuchObjectException o1, 2:MetaException o2, 3:InvalidObjectException o3,
                            4:InvalidInputException o4)
               bool delete_table_column_statistics(1:string db_name, 2:string tbl_name, 3:string col_name) throws
                           (1:NoSuchObjectException o1, 2:MetaException o2, 3:InvalidObjectException o3,
                            4:InvalidInputException o4)

               //
               // user-defined functions
               //

               void create_function(1:Function func)
                   throws (1:AlreadyExistsException o1,
                           2:InvalidObjectException o2,
                           3:MetaException o3,
                           4:NoSuchObjectException o4)

               void drop_function(1:string dbName, 2:string funcName)
                   throws (1:NoSuchObjectException o1, 2:MetaException o3)

               void alter_function(1:string dbName, 2:string funcName, 3:Function newFunc)
                   throws (1:InvalidOperationException o1, 2:MetaException o2)

               list<string> get_functions(1:string dbName, 2:string pattern)
                   throws (1:MetaException o1)
               Function get_function(1:string dbName, 2:string funcName)
                   throws (1:MetaException o1, 2:NoSuchObjectException o2)

               //authorization privileges

               bool create_role(1:Role role) throws(1:MetaException o1)
               bool drop_role(1:string role_name) throws(1:MetaException o1)
               list<string> get_role_names() throws(1:MetaException o1)
               // Deprecated, use grant_revoke_role()
               bool grant_role(1:string role_name, 2:string principal_name, 3:PrincipalType principal_type,
                 4:string grantor, 5:PrincipalType grantorType, 6:bool grant_option) throws(1:MetaException o1)
               // Deprecated, use grant_revoke_role()
               bool revoke_role(1:string role_name, 2:string principal_name, 3:PrincipalType principal_type)
                                     throws(1:MetaException o1)
               list<Role> list_roles(1:string principal_name, 2:PrincipalType principal_type) throws(1:MetaException o1)
               GrantRevokeRoleResponse grant_revoke_role(1:GrantRevokeRoleRequest request) throws(1:MetaException o1)

               // get all role-grants for users/roles that have been granted the given role
               // Note that in the returned list of RolePrincipalGrants, the roleName is
               // redundant as it would match the role_name argument of this function
               GetPrincipalsInRoleResponse get_principals_in_role(1: GetPrincipalsInRoleRequest request) throws(1:MetaException o1)

               // get grant information of all roles granted to the given principal
               // Note that in the returned list of RolePrincipalGrants, the principal name,type is
               // redundant as it would match the principal name,type arguments of this function
               GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(1: GetRoleGrantsForPrincipalRequest request) throws(1:MetaException o1)

               PrincipalPrivilegeSet get_privilege_set(1:HiveObjectRef hiveObject, 2:string user_name,
                 3: list<string> group_names) throws(1:MetaException o1)
               list<HiveObjectPrivilege> list_privileges(1:string principal_name, 2:PrincipalType principal_type,
                 3: HiveObjectRef hiveObject) throws(1:MetaException o1)

               // Deprecated, use grant_revoke_privileges()
               bool grant_privileges(1:PrivilegeBag privileges) throws(1:MetaException o1)
               // Deprecated, use grant_revoke_privileges()
               bool revoke_privileges(1:PrivilegeBag privileges) throws(1:MetaException o1)
               GrantRevokePrivilegeResponse grant_revoke_privileges(1:GrantRevokePrivilegeRequest request) throws(1:MetaException o1);

               // this is used by metastore client to send UGI information to metastore server immediately
               // after setting up a connection.
               list<string> set_ugi(1:string user_name, 2:list<string> group_names) throws (1:MetaException o1)

               //Authentication (delegation token) interfaces

               // get metastore server delegation token for use from the map/reduce tasks to authenticate
               // to metastore server
               string get_delegation_token(1:string token_owner, 2:string renewer_kerberos_principal_name)
                 throws (1:MetaException o1)

               // method to renew delegation token obtained from metastore server
               i64 renew_delegation_token(1:string token_str_form) throws (1:MetaException o1)

               // method to cancel delegation token obtained from metastore server
               void cancel_delegation_token(1:string token_str_form) throws (1:MetaException o1)

               // Transaction and lock management calls
               // Get just list of open transactions
               GetOpenTxnsResponse get_open_txns()
               // Get list of open transactions with state (open, aborted)
               GetOpenTxnsInfoResponse get_open_txns_info()
               OpenTxnsResponse open_txns(1:OpenTxnRequest rqst)
               void abort_txn(1:AbortTxnRequest rqst) throws (1:NoSuchTxnException o1)
               void commit_txn(1:CommitTxnRequest rqst) throws (1:NoSuchTxnException o1, 2:TxnAbortedException o2)
               LockResponse lock(1:LockRequest rqst) throws (1:NoSuchTxnException o1, 2:TxnAbortedException o2)
               LockResponse check_lock(1:CheckLockRequest rqst)
                 throws (1:NoSuchTxnException o1, 2:TxnAbortedException o2, 3:NoSuchLockException o3)
               void unlock(1:UnlockRequest rqst) throws (1:NoSuchLockException o1, 2:TxnOpenException o2)
               ShowLocksResponse show_locks(1:ShowLocksRequest rqst)
               void heartbeat(1:HeartbeatRequest ids) throws (1:NoSuchLockException o1, 2:NoSuchTxnException o2, 3:TxnAbortedException o3)
               HeartbeatTxnRangeResponse heartbeat_txn_range(1:HeartbeatTxnRangeRequest txns)
               void compact(1:CompactionRequest rqst)
               ShowCompactResponse show_compact(1:ShowCompactRequest rqst)
             }""")
    Assert.assertTrue(r.successful)

  }

}
