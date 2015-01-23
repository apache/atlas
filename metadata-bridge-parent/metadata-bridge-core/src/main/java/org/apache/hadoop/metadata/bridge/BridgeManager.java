package org.apache.hadoop.metadata.bridge;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Properties;

import javax.inject.Inject;

import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.apache.hadoop.metadata.types.AttributeDefinition;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.TypeSystem;


public class BridgeManager {
	TypeSystem ts;
	ArrayList<ABridge> activeBridges;
	private final static String bridgeFileDefault = "bridge-manager.properties"; 
	
	@Inject
	BridgeManager(MetadataRepository rs){
		this.ts = TypeSystem.getInstance();
		if(System.getProperty("bridgeManager.propsFile") != null | !System.getProperty("bridgeManager.propsFile").isEmpty()){
			setActiveBridges(System.getProperty("bridgeManager.propsFile"));	
		}else{
			setActiveBridges(System.getProperty(bridgeFileDefault));
		}
		
		for (ABridge bridge : activeBridges){
			try {
				this.loadTypes(bridge, ts);
			} catch (MetadataException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// Handle some kind of errors - waiting on errors concept from typesystem
	}
	
	public ArrayList<ABridge> getActiveBridges(){
		return this.activeBridges;
	}
	
	private void setActiveBridges(String bridgePropFileName){
			if(bridgePropFileName == null | bridgePropFileName.isEmpty()){
				bridgePropFileName = BridgeManager.bridgeFileDefault;
			}
		ArrayList<ABridge> aBList = new ArrayList<ABridge>();
		Properties props = new Properties();
		InputStream configStm = this.getClass().getResourceAsStream(bridgePropFileName);
		try {
			ABridge.LOG.info("Loading : Active Bridge List");
			props.load(configStm);
			String[] activeBridgeList = ((String)props.get("BridgeManager.activeBridges")).split(",");
			ABridge.LOG.info("Loaded : Active Bridge List");
			ABridge.LOG.info("First Loaded :" + activeBridgeList[0]);
			
			for (String s : activeBridgeList){
				Class<?> bridgeCls = (Class<?>) Class.forName(s);
				if(bridgeCls.isAssignableFrom(ABridge.class)){
					aBList.add((ABridge) bridgeCls.newInstance());
				}
			}
			
		} catch (IOException e) {
			ABridge.LOG.error(e.getMessage(), e);
			e.printStackTrace();
		} catch (InstantiationException e) {
			ABridge.LOG.error(e.getMessage(), e);
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			ABridge.LOG.error(e.getMessage(), e);
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			ABridge.LOG.error(e.getMessage(), e);
			e.printStackTrace();
		}
		this.activeBridges = aBList;
		
		
		
	}
	
	private final boolean loadTypes(ABridge bridge, TypeSystem ts) throws MetadataException{
		for (Class<AEnitityBean> clazz : bridge.getTypeBeanClasses()){
			ts.defineClassType(BridgeManager.convertEntityBeanToClassTypeDefinition(clazz));
		}
		return false;
		
		
	}
	
	public final static HierarchicalTypeDefinition<ClassType> convertEntityBeanToClassTypeDefinition(Class<? extends AEnitityBean> class1){   
		ArrayList<AttributeDefinition> attDefAL = new ArrayList<AttributeDefinition>();
		for (Field f: class1.getFields()){
			try {
				attDefAL.add(BridgeManager.convertFieldtoAttributeDefiniton(f));
			} catch (MetadataException e) {
				ABridge.LOG.error("Class " + class1.getName() + " cannot be converted to TypeDefinition");
				e.printStackTrace();
			}
		}
		
		HierarchicalTypeDefinition<ClassType> typeDef = new HierarchicalTypeDefinition<>(ClassType.class, class1.getSimpleName(),
                null, (AttributeDefinition[])attDefAL.toArray());
		
		return typeDef;
	}
	
	public final static AttributeDefinition convertFieldtoAttributeDefiniton(Field f) throws MetadataException{
		
		return new AttributeDefinition(f.getName(), f.getType().getSimpleName(), Multiplicity.REQUIRED, false, null);
	}
	
}
