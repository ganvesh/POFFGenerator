package com.jci.poffgenerator.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.jci.poffgenerator.dao.POFFGeneratorDAOImpl;

@Service
public class POFFGeneratorServiceImpl implements POFFGeneratorService {

	@Autowired 
	POFFGeneratorDAOImpl poffGeneratorDAOImpl;
	
	public Map<String, List<HashMap<String, Object>>> getPoDetails() {
		return poffGeneratorDAOImpl.getPoDetails();
	}

	public List<String> getPoItemsDetailsFlatFile() {
		return poffGeneratorDAOImpl.getPoItemsDetailsFlatFile();
	}

}
