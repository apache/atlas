package org.apache.metadata.dsl

import org.apache.metadata.storage.TypedStruct
import scala.language.dynamics

class DynamicTypedStruct(val ts : TypedStruct) extends Dynamic {
  def selectDynamic(name: String) = ts.get(name)
  def updateDynamic(name: String)(value: Any) {
    var value1 = value
    if ( value != null && value.isInstanceOf[DynamicTypedStruct]) {
      value1 = value.asInstanceOf[DynamicTypedStruct].ts
    }
    ts.set(name, value1)
  }
  def dataType = ts.dataType
}
