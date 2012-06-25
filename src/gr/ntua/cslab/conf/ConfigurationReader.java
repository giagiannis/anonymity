package gr.ntua.cslab.conf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class ConfigurationReader {

	private String defaultFile="configuration.conf";
	private String file;
	public ConfigurationReader(){
		this.file=defaultFile;
	}
	
	public ConfigurationReader(String file){
		this.file=file;
	}
		
	public String getProperty(String property) throws IOException{
		BufferedReader reader = new BufferedReader(new FileReader(this.file));
		String buffer=new String();
		while(reader.ready()){
			buffer = reader.readLine();
			if(buffer.startsWith(property)){
				reader.close();
				return buffer.substring(buffer.indexOf(property)+property.length(),buffer.length()).trim();
			}
		}
		reader.close();
		return null;
	}
}
