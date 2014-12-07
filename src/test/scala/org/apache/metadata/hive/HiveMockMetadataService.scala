package org.apache.metadata.hive


object HiveMockMetadataService {

  case class FieldSchema(name : String, typeName : String, comment : String)

  case class SerDe(name : String, serializationLib : String, parameters : Map[String, String])

  case class StorageDescriptor(fields : List[FieldSchema],
                               location : String, inputFormat : String,
                                outputFormat : String, compressed : Boolean,
                                numBuckets : Int, bucketColumns : List[String],
                                sortColumns : List[String],
                                parameters : Map[String, String],
                                storedAsSubDirs : Boolean
                                )

  case class Table(dbName : String, tableName : String, storageDesc : StorageDescriptor,
                   parameters : Map[String, String],
                    tableType : String)


  def getTable(dbName : String, table : String) : Table = {
    return Table(dbName, table,
      StorageDescriptor(
      List[FieldSchema](
        FieldSchema("d_date_sk", "int", null),
        FieldSchema("d_date_id", "string", null),
        FieldSchema("d_date", "string", null),
        FieldSchema("d_month_seq", "int", null),
        FieldSchema("d_week_seq", "int", null),
        FieldSchema("d_quarter_seq", "int", null),
        FieldSchema("d_year", "int", null),
        FieldSchema("d_dow", "int", null),
        FieldSchema("d_moy", "int", null),
        FieldSchema("d_dom", "int", null),
        FieldSchema("d_qoy", "int", null),
        FieldSchema("d_fy_year", "int", null),
        FieldSchema("d_fy_quarter_seq", "int", null),
        FieldSchema("d_fy_week_seq", "int", null),
        FieldSchema("d_day_name", "string", null),
        FieldSchema("d_quarter_name", "string", null),
        FieldSchema("d_holiday", "string", null),
        FieldSchema("d_weekend", "string", null),
        FieldSchema("d_following_holiday", "string", null),
        FieldSchema("d_first_dom", "int", null),
        FieldSchema("d_last_dom", "int", null),
        FieldSchema("d_same_day_ly", "int", null),
        FieldSchema("d_same_day_lq", "int", null),
        FieldSchema("d_current_day", "string", null),
        FieldSchema("d_current_week", "string", null),
        FieldSchema("d_current_month", "string", null),
        FieldSchema("d_current_quarter", "string", null),
        FieldSchema("d_current_year", "string", null)
      ),
      "file:/tmp/warehouse/tpcds.db/date_dim",
      "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
      "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
      false,
      0,List[String](), List[String](),
        Map[String,String](),
      false
      ),
    Map[String,String](),
    "Table")
  }
}
