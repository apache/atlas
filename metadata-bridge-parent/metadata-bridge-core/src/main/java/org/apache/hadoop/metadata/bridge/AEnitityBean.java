package org.apache.hadoop.metadata.bridge;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Map.Entry;

import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.Referenceable;
import org.apache.hadoop.metadata.types.AttributeInfo;

public abstract class AEnitityBean {

	
	public final Referenceable convertToReferencable() throws IllegalArgumentException, IllegalAccessException{
		Referenceable selfAware = new Referenceable(this.getClass().getSimpleName());
		for(Field f : this.getClass().getFields()){
			selfAware.set(f.getName(), f.get(this));
		}
		return selfAware;
	}
	
	public final <t extends AEnitityBean>Object convertFromITypedReferenceable(ITypedReferenceableInstance instance) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, BridgeException{
		if(!instance.getTypeName().equals(this.getClass().getSimpleName())){
			throw new BridgeException("ReferenceableInstance type not the same as bean");
		}
		Object retObj = this.getClass().newInstance();
		for (Entry<String, AttributeInfo> e : instance.fieldMapping().fields.entrySet()){
			try {
				
				String convertedName = e.getKey().substring(0, 1).toUpperCase()+e.getKey().substring(1);
				this.getClass().getMethod("set"+convertedName, Class.forName(e.getValue().dataType().getName())).invoke(this, instance.get(e.getKey()));
			} catch (MetadataException | ClassNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		return retObj;
	}
	
}
