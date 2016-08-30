package com.jci.poffgenerator.service;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface POFFGeneratorService {

	Map<String,List<HashMap<String, Object>>> getPoDetails();
	
	LinkedHashMap<String, List<String>> getPoItemsDetailsFlatFile();
}
