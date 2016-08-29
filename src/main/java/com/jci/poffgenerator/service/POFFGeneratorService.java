package com.jci.poffgenerator.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface POFFGeneratorService {

	public Map<String,List<HashMap<String, Object>>> getPoDetails();
	
	public List<String> getPoItemsDetailsFlatFile();
}
