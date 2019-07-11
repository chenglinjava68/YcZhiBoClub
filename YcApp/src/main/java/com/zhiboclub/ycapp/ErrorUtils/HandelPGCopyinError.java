package com.zhiboclub.ycapp.ErrorUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class HandelPGCopyinError {
	
	private static Logger LOG = LoggerFactory.getLogger(HandelPGCopyinError.class);
	

	private static final String SPLITCHAR = "\u0001,\u0001";
	
	/**
	 * 列出某个文件路径下其中包含某个特定字符串的文件列表
	 * @param path 所遍历的路径
	 * @param regex 包含的特定字符串
	 * @return
	 */
	public List<File> ListFileWithRegex(String path,String regex) {
		File filepath = new File(path);
		List<File> listNeedsFiles = new ArrayList<File>();
		File[] list_error_file = filepath.listFiles();
		for (File file:list_error_file) {
			if(file.getName().contains(regex)) {
				listNeedsFiles.add(file);
			}
		}
		return listNeedsFiles;
	}
	
	public void readFile(String filename) {
		try (BufferedReader br = new BufferedReader(new FileReader(filename))){
			String line;
            while ((line = br.readLine()) != null) {
            	System.out.println(line.split(HandelPGCopyinError.SPLITCHAR)[line.split(HandelPGCopyinError.SPLITCHAR).length -1 ]);
            }
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		HandelPGCopyinError handle = new HandelPGCopyinError();
		List<File> files = handle.ListFileWithRegex("logs", "errordata");
		for (File file :files) {
			LOG.info("读取的文件为："+ file.getName());
			handle.readFile(file.getPath());
		}
	}
}
