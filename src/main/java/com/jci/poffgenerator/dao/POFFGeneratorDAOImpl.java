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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
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
	 
	  @Value("${flat.file.name.sender.duns}")
	  private  String senderDuns;
	 
	  @Value("${flat.file.name.receiver.duns}")
	  private  String receiverDuns;
	  
	  @Value("${flat.file.name.message.type}")
	  private   String messageType;
	  
	  @Value("${flat.file.name.version}")
	  private   String version;
	  
	  @Value("${flat.file.name.site.id}")
	  private  String siteId;
	  
	  @Value("${flat.file.name.date.format}")
	  private  String dateFormat;
	  
	  @Value("${flat.file.name.date.time.zone}")
	  private  String timeZone;
	  
	  String orderNumber = "";
	  
	  

	public Map<String,List<HashMap<String, Object>>> getPoDetails(){
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
		}
	    return poNumToItemListMap;
	}
	
	
	
	public List<String> getPoItemsDetailsFlatFile(){
	    List<LinkedHashMap<String, Object>> resultList = new ArrayList<LinkedHashMap<String, Object>>();
	    LinkedHashMap<String, Object> resultMap = new LinkedHashMap<String, Object>();
		Map<String,List<HashMap<String, Object>>> poNumToItemListMap = getPoDetails();
		List<String> tabFormatList = new ArrayList<String>();
		
		for(Map.Entry entry:poNumToItemListMap.entrySet()){
			List<HashMap<String, Object>> list = (List<HashMap<String, Object>>) entry.getValue();
			String deliveryState = "";
			EntityProperty ep = null;
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
								resultMap = headerMapping(ep.getValueAsString());
								resultList.add(resultMap);
								String tabFormatString = fixedLengthString(resultMap).toString();
								tabFormatList.add(tabFormatString);
							}
						}
				    }
				}catch (Exception e){
				    e.printStackTrace();
				}
			}
		}
		return tabFormatList;
	}
	
	public  String getFileName() {
		StringBuilder name = new StringBuilder();
		name.append(orderNumber);
		name.append(".");
		name.append(senderDuns);
		name.append("_");
		name.append(receiverDuns);
		name.append("_");
		name.append(messageType);
		name.append("_");
		name.append(version);
		name.append("_");
		name.append(siteId);
		name.append("_");
		
		SimpleDateFormat isoFormat = new SimpleDateFormat(dateFormat);
		isoFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
		String timestamp =isoFormat.format(new Date());
		name.append(timestamp);
		return name.toString();
	}
	
	public LinkedHashMap<String, Object> headerMapping(String jsonString){
		LinkedHashMap<String, Object> resultMap = new LinkedHashMap<String, Object>();
		try{
		   JSONParser parser = new JSONParser();
		   HashMap<String,String> headerMapping = (HashMap<String,String>) parser.parse(new FileReader(jsonFileName));
		   for(int i=0;i<headerMapping.size();i++){
			   HashMap<String, Object> obj = (HashMap<String, Object>) parser.parse(jsonString);
			   resultMap.put(headerMapping.get(Integer.toString(i)), obj.get(headerMapping.get(Integer.toString(i))));
		   }
		}catch(Exception e){
			e.printStackTrace();
		}
		return resultMap;
	}
	
	public StringBuilder fixedLengthString(LinkedHashMap<String, Object> map){
		StringBuilder line = new StringBuilder();
		int i = 0;
		int size = map.size();
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
		return line;
	}

public static boolean isBlank(String val){
	if("null".equals(val) || StringUtils.isBlank(val) || (val == null)){
		return true;
	}
	return false;
}



public static String appendTab(Object value) {
	if(value==null || "".equals(value) || "null".equals(value)){
		return "\t";
	}else{
		return value+"\t";
	}
}
}
