package com.zhiboclub.ycapp.ErrorUtils;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import com.zhiboclub.ycapp.Utils.TimeManager;

public class CopyInErrorToFile {

	public void AbortDataToFile(String errorData) {
		BufferedWriter out = null;
		try {
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("logs/errordata_" + TimeManager.getFileTimeStr() + ".log", true)));
			out.write(errorData);
			out.flush();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
