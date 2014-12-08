package org.apache.metadata.dsl

import org.apache.metadata.MetadataService
import org.apache.metadata.storage.StructInstance
import org.apache.metadata.types.TypeSystem
import scala.language.dynamics

class DynamicTypedStruct(val ts : StructInstance) extends Dynamic {
  def selectDynamic(name: String) = ts.get(name)
  def updateDynamic(name: String)(value: Any) {
    var value1 = value
    if ( value != null && value.isInstanceOf[DynamicTypedStruct]) {
      value1 = value.asInstanceOf[DynamicTypedStruct].ts
    }
    ts.set(name, value1)
  }
  def dataType = MetadataService.getCurrentTypeSystem.getDataType(ts.getTypeName)
}
