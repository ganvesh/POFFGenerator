package com.jci.poffgenerator.dao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface POFFGeneratorDAO {

	public Map<String,List<HashMap<String, Object>>> getPoDetails();
	
	public List<String> getPoItemsDetailsFlatFile();
}
