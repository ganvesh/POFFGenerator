package com.jci.poffgenerator.service;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.jci.poffgenerator.dao.POFFGeneratorDAO;

@Service
public class POFFGeneratorServiceImpl implements POFFGeneratorService {

	@Autowired 
	POFFGeneratorDAO poffGeneratorDAO;
	
	public Map<String, List<HashMap<String, Object>>> getPoDetails() {
		return poffGeneratorDAO.getPoDetails();
	}

	public LinkedHashMap<String, List<String>> getPoItemsDetailsFlatFile() {
		return poffGeneratorDAO.getPoItemsDetailsFlatFile();
	}

}
