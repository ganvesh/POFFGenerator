package com.jci.poffgenerator.dao;

import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import com.jci.poffgenerator.controller.POFFGeneratorController;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.DynamicTableEntity;
import com.microsoft.azure.storage.table.EntityProperty;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableQuery.Operators;
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons;

@Configuration
@Service
@SuppressWarnings({ "unchecked", "unused", "rawtypes" })
public class POFFGeneratorDAOImpl implements POFFGeneratorDAO {
	
	  @Value("${azure.storage.connectionstring}")
	  private  String storageConnectionString;
	  
	  @Value("${azure.storage.potablename}")
	  private  String poTableName;
	  
	  @Value("${azure.storage.poitemtablename}")
	  private  String poItemTableName;
	  
	  @Value("${azure.storage.partionkey.po}")
	  private String poPartitionKey;
	  
	  @Value("${azure.storage.partionkey.po_item}")
	  private String poItemPartitionKey;
	  
	  @Value("${azure.storage.pocolumn.supplierdeliverystatecolumn}")
	  private String poColumnSupplierDelivery;
	  
	  @Value("${azure.storage.pocolumnvalue.supplierdeliverystatevalue}")
	  private Integer poColumnSupplierDeliveryValue;
	  
	  @Value("${azure.storage.poitemdetails.jsonstringkey}")
	  private String itemsStringKey;
	  
	  @Value("${azure.storage.pocolumn.ordernumbercolumn}")
	  private String orderNumberColumn;
	  
	  @Value("${azure.storage.mapping.jsonfilename}")
	  private String jsonFileName;
	  
	  @Value("${flat.file.destination.mapping.folder.url}")
	  private  String folderUrl;
	  	 
	  String orderNumber = "";
	  
	  private static final Logger LOG = LoggerFactory.getLogger(POFFGeneratorDAOImpl.class);
	  

	public Map<String,List<HashMap<String, Object>>> getPoDetails(){
		LOG.info("### Fetching POFFGeneratorDAOImpl.getPoDetails() Begins ####");
	    EntityProperty ep;
	    Map<String,List<HashMap<String, Object>>> poNumToItemListMap = new HashMap<String,List<HashMap<String, Object>>>();
	    
	    try{
		    CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
		    CloudTableClient tableClient = storageAccount.createCloudTableClient();
		    CloudTable cloudTable = tableClient.getTableReference(poTableName);
		    String supplierDeliveryFilter = TableQuery.generateFilterCondition(poColumnSupplierDelivery, QueryComparisons.EQUAL, poColumnSupplierDeliveryValue);
		    TableQuery<DynamicTableEntity> poQuery = TableQuery.from(DynamicTableEntity.class).where(supplierDeliveryFilter);
		   for (DynamicTableEntity entity : cloudTable.execute(poQuery)) {
		    	HashMap<String, Object> hashMap = new HashMap<String, Object>();
		    	HashMap<String, EntityProperty> props = entity.getProperties();
		    	for (String key : props.keySet()) {
					ep = props.get(key);
					hashMap.put(key, ep.getValueAsString());
				}
		    	if(poNumToItemListMap.containsKey(entity.getRowKey().split("_")[0])){
					List<HashMap<String, Object>> list =poNumToItemListMap.get(entity.getRowKey().split("_")[0]);
		    		list.add(hashMap);
		    		poNumToItemListMap.put(entity.getRowKey().split("_")[0], list);
		    	}else{
		    		List<HashMap<String, Object>> list = new  ArrayList<HashMap<String, Object>>();
		    		list.add(hashMap);
		    		poNumToItemListMap.put(entity.getRowKey().split("_")[0], list);
		    	}		    	
		   }
		}
		catch (Exception e){
		    e.printStackTrace();
		    LOG.error("Error in Fetching POFFGeneratorDAOImpl.getPoDetails() Error: "+e.getMessage());
		}
	    LOG.info("### Fetching POFFGeneratorDAOImpl.getPoDetails() Ends ####");
	    return poNumToItemListMap;
	}
	
	public LinkedHashMap<String, List<String>> getPoItemsDetailsFlatFile(){
		LOG.info("### Fetching POFFGeneratorDAOImpl.getPoItemsDetailsFlatFile() Begins ####");
	    List<LinkedHashMap<String, Object>> resultList = new ArrayList<LinkedHashMap<String, Object>>();
	    LinkedHashMap<String, List<String>> multiplePoMap = new LinkedHashMap<String, List<String>>();
	    LinkedHashMap<String, Object> singlePoMap = new LinkedHashMap<String, Object>();
		Map<String,List<HashMap<String, Object>>> poNumToItemListMap = getPoDetails();
		
		for(Map.Entry entry:poNumToItemListMap.entrySet()){
			List<HashMap<String, Object>> list = (List<HashMap<String, Object>>) entry.getValue();
			String deliveryState = "";
			EntityProperty ep = null;
			List<String> singlePotabFormatList = new ArrayList<String>();
			for(int i=0;i<list.size();i++){
				deliveryState = list.get(i).get(poColumnSupplierDelivery).toString();
				orderNumber = list.get(i).get("OrderNumber").toString();
				try
				{
				   CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
				   CloudTableClient tableClient = storageAccount.createCloudTableClient();
				   CloudTable cloudTable = tableClient.getTableReference(poItemTableName);
				   String orderFilter = TableQuery.generateFilterCondition(orderNumberColumn, QueryComparisons.EQUAL, orderNumber);
				   TableQuery<DynamicTableEntity> tableQuery = TableQuery.from(DynamicTableEntity.class).where(orderFilter);
				    for (DynamicTableEntity entity : cloudTable.execute(tableQuery)) {
				    	HashMap<String, EntityProperty> props = entity.getProperties();
				    	for (String key : props.keySet()) {
							ep = props.get(key);
							if(key.equals(itemsStringKey)){
								singlePoMap = headerMapping(ep.getValueAsString());
								resultList.add(singlePoMap);
								String tabFormatString = fixedLengthString(singlePoMap).toString();
								singlePotabFormatList.add(tabFormatString);
							}
						}
				    }
				}catch (Exception e){
					LOG.error("### Fetching POFFGeneratorDAOImpl.getPoItemsDetailsFlatFile() Error: "+e.getMessage());
				    e.printStackTrace();
				}
			}
			multiplePoMap.put(orderNumber, singlePotabFormatList);
		}
		LOG.info("### Fetching POFFGeneratorDAOImpl.getPoItemsDetailsFlatFile() Ends ####");
		return multiplePoMap;
	}
	
	public LinkedHashMap<String, Object> headerMapping(String jsonString){
		LOG.info("### Mapping POItemDetails Begins ####");
		LinkedHashMap<String, Object> singlePoMap = new LinkedHashMap<String, Object>();
		try{
		   JSONParser parser = new JSONParser();
		   HashMap<String,String> headerMapping = (HashMap<String,String>) parser.parse(new FileReader(jsonFileName));
		   for(int i=0;i<headerMapping.size();i++){
			   HashMap<String, Object> obj = (HashMap<String, Object>) parser.parse(jsonString);
			   singlePoMap.put(headerMapping.get(Integer.toString(i)), obj.get(headerMapping.get(Integer.toString(i))));
		   }
		}catch(Exception e){
			LOG.error("Exception in Mapping POItemDetails: "+e.getMessage());
			e.printStackTrace();
		}
		LOG.info("### Mapping POItemDetails Ends ####");
		return singlePoMap;
	}
	
	public StringBuilder fixedLengthString(LinkedHashMap<String, Object> map){
		StringBuilder line = new StringBuilder();
		int i = 0;
		int size = map.size();
		try{
				for(Map.Entry<String, Object> mapEntry : map.entrySet()){
					if((size-1)==i){
							if(isBlank(String.valueOf((mapEntry.getValue() == null) ? "":mapEntry.getValue().toString()))){
								line.append(appendTab(""));
							}else{
								line.append(appendTab(mapEntry.getValue()));
							}
					 }else{
						 if(isBlank(String.valueOf((mapEntry.getValue() == null) ? "":mapEntry.getValue().toString()))){
								line.append(appendTab(""));
							}else{
								line.append(appendTab(mapEntry.getValue()));
							}
					 }
					i++;
				}
		}catch(Exception e){
			LOG.error("Exception while mapping in POFFGeneratorDAOImpl.FixedLengthString(): "+e.getMessage());
			e.printStackTrace();
		}
		return line;
	}

	private boolean isBlank(String val){
		if("null".equals(val) || StringUtils.isBlank(val) || (val == null)){
			return true;
		}
		return false;
	}
	
	private String appendTab(Object value) {
		if(value==null || "".equals(value) || "null".equals(value)){
			return "\t";
		}else{
			return value+"\t";
		}
	}

}
