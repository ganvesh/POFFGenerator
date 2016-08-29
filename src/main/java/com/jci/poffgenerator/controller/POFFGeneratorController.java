package com.jci.poffgenerator.controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.jci.poffgenerator.dao.POFFGeneratorDAOImpl;
import com.jci.poffgenerator.service.POFFGeneratorServiceImpl;

@Configuration
@RestController
public class POFFGeneratorController {
	
	@Value("${flat.file.destination.supplier.url}")
	private  String supplierUrl;
	 
	
	@Autowired
	POFFGeneratorServiceImpl poffGeneratorServiceImpl;
	
	@Autowired
	POFFGeneratorDAOImpl poffGeneratorDAOImpl;

	@Scheduled(fixedDelay = 6000) 
	@RequestMapping(value="/poFFGenerator", method = RequestMethod.GET)
	@ResponseBody
	public String poData(){
		System.out.println("*******************");
		List<String> flatFileLines = poffGeneratorServiceImpl.getPoItemsDetailsFlatFile();
		File toFile=null;
		try {
			toFile = File.createTempFile(poffGeneratorDAOImpl.getFileName(), ".txt");
	    	FileUtils.writeLines(toFile,"UTF-8", flatFileLines,false);
		} catch (IOException e) {
			e.printStackTrace();
		}
		String fileName = toFile.getName();
		
		String mimeType= URLConnection.guessContentTypeFromName(toFile.getName());
		 InputStream input =null; 
		 //start
		 try{
			 RestTemplate template = new RestTemplate();
			 MultiValueMap<String, Object> requestMap = new LinkedMultiValueMap<String, Object>();
			 requestMap.add("name", toFile.getName());
			 requestMap.add("filename", toFile.getName());
			 requestMap.set("Content-Type", mimeType);
			 requestMap.set("Content-Length",(int)toFile.length());			 
			 
			 input = new FileInputStream(toFile);
			 ByteArrayResource contentsAsResource = new ByteArrayResource(IOUtils.toByteArray(input)){
			             @Override
			             public String getFilename(){
			                 return fileName;
			             }
			 };
			 requestMap.add("file", contentsAsResource);
			 String response =  template.postForObject((supplierUrl+"?filename="+poffGeneratorDAOImpl.getFileName()), requestMap, String.class);
			 System.out.println(response);
		 }catch(Exception e) {
			e.printStackTrace();
		}finally{
			try {
				input.close();
				FileUtils.forceDelete(toFile);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			
		}
		
		return "success";
	}
	
	
	
}

