package com.jci.poffgenerator.controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.jci.poffgenerator.service.POFFGeneratorService;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

@Configuration
@RestController
public class POFFGeneratorController {
	
	private static final Logger LOG = LoggerFactory.getLogger(POFFGeneratorController.class);
	
	  @Value("${flat.file.destination.supplier.url}")
	  private  String supplierUrl;
	
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
	 
	
	@Autowired
	POFFGeneratorService poffGeneratorService;
	
	@Scheduled(fixedDelay = 6000) 
	@RequestMapping(value="/poFFGenerator", method = RequestMethod.GET)
	@ResponseBody
	public HashMap<String, String> poFFGenerator(){
		LOG.info("### Starting poFFGenerator controller ####");
		HashMap<String, String> resultPOFileMap = new HashMap<String, String>();
		LOG.info("Call FlatFile Service poffGeneratorServiceImpl.getPoItemsDetailsFlatFile() begins");
		LinkedHashMap<String, List<String>> resultPOFFMapper = poffGeneratorService.getPoItemsDetailsFlatFile();
		LOG.info("Received PO Item Details from poffGeneratorServiceImpl.getPoItemsDetailsFlatFile()");
		for(Map.Entry<String, List<String>> mapresp: resultPOFFMapper.entrySet()){
			LOG.info("FlatFile Generation For PONumber: "+mapresp.getKey()+" Begins.");
			String poFileName = getFileName(mapresp.getKey());
			File toFile=null;
			try {
				LOG.info("Create Temporary File.");
				toFile = File.createTempFile(poFileName, ".txt");
		    	FileUtils.writeLines(toFile,"UTF-8", mapresp.getValue(),false);
			} catch (IOException e) {
				LOG.error("Exception in createing temporary file message:"+e.getMessage());
				LOG.error("stack trace:",e);
			}
			String fileName = toFile.getName();
			
			String mimeType= URLConnection.guessContentTypeFromName(toFile.getName());
			 InputStream input =null; 
			 //start
			 try{
				 LOG.info("RestTemplate RequestMap Starts");
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
				 LOG.info("RestTemplate Call Begins");
				 //String response =  template.postForObject((supplierUrl+"?filename="+poFileName), requestMap, String.class);
				 LOG.info("RestTemplate Call Ends");
				 //System.out.println(response);
				 resultPOFileMap.put(mapresp.getKey(), "success");
				 LOG.info("RestTemplate RequestMap Ends");
				 send(mimeType, (int)toFile.length());
			       /* try {
			            URL url = new URL("sftp://10.11.9.23:443");
			            URLConnection conn = url.openConnection();
			            OutputStream outputStream = conn.getOutputStream();
			            outputStream.write((int)toFile.length());
			            outputStream.close();
			 
			            System.out.println("File uploaded");
			        } catch (IOException ex) {
			            ex.printStackTrace();
			        }*/
			 }catch(Exception e) {
				 LOG.error("Error in FlatFile Generation For PONumber: "+mapresp.getKey()+" "+e.getMessage());
				 e.printStackTrace();
				 resultPOFileMap.put(mapresp.getKey(), "failed");
			}finally{
				try {
					input.close();
					FileUtils.forceDelete(toFile);
					LOG.info("Input Stream Resource Closed");
				} catch (IOException e1) {
					LOG.error("Error in closing input stream"+ e1.getMessage());
					e1.printStackTrace();
				}
				
			}
			 LOG.info("FlatFile Generation For PONumber: "+mapresp.getKey()+" Ends.");
		}
		LOG.info("### Ending poFFGenerator controller ####");
		return resultPOFileMap;
	}
	
	public  String getFileName(String poNumber) {
		StringBuilder name = new StringBuilder();
		name.append(poNumber);
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
	
	public  void send (String mimeType, int streamData) {
        String SFTPHOST = "10.11.9.23";
        int SFTPPORT = 443;
        String SFTPUSER = "";
        String SFTPPASS = "";
        String SFTPWORKINGDIR = "file/to/transfer";

        Session session = null;
        Channel channel = null;
        ChannelSftp channelSftp = null;
        System.out.println("preparing the host information for sftp.");
        try {
            JSch jsch = new JSch();
            session = jsch.getSession(SFTPUSER, SFTPHOST, SFTPPORT);
            session.setPassword(SFTPPASS);
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();
            System.out.println("Host connected.");
            channel = session.openChannel("sftp");
            channel.connect();
            System.out.println("sftp channel opened and connected.");
            channelSftp = (ChannelSftp) channel;
            channelSftp.put("Content-Type", mimeType);
            channelSftp.put("Content-Length", streamData);
            //channelSftp.put\
            System.out.println("File transfered successfully to host.");
        } catch (Exception ex) {
             System.out.println("Exception found while tranfer the response.");
             ex.printStackTrace();
        }
        finally{
            channelSftp.exit();
            System.out.println("sftp Channel exited.");
            channel.disconnect();
            System.out.println("Channel disconnected.");
            session.disconnect();
            System.out.println("Host Session disconnected.");
        }
    }   
	
	
	
}

