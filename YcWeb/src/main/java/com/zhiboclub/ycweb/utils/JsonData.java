package com.zhiboclub.ycweb.utils;

import java.io.Serializable;
import java.util.List;

public class JsonData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Integer code; // 状态码 0 表示成功，1表示处理中，-1表示失败
	private Object data; // 数据
	private String msg;// 描述
	private int currentpage;// 当前页
	private int pagesize;// 页面显示数量
	private int totalnum;// 总数

	public JsonData() {
	}

	public JsonData(Integer code, Object data, String msg) {
		this.code = code;
		this.data = data;
		this.msg = msg;
	}

	public JsonData(Integer code, Object data, String msg, int currentpage, int pagesize, int totalnum) {
		this.code = code;
		this.data = data;
		this.msg = msg;
		this.currentpage = currentpage;
		this.pagesize = pagesize;
		this.totalnum = totalnum;
	}

	// 成功，传入数据
	public static JsonData buildSuccess() {
		return new JsonData(0, null, null);
	}

	// 成功，传入数据
	public static JsonData buildSuccess(Object data) {
		return new JsonData(0, data, null);
	}

	// 失败，传入描述信息
	public static JsonData buildError(String msg) {
		return new JsonData(-1, null, msg);
	}

	// 失败，传入描述信息,状态码
	public static JsonData buildError(String msg, Integer code) {
		return new JsonData(code, null, msg);
	}

	// 成功，传入数据,及描述信息
	public static JsonData buildSuccess(Object data, String msg) {
		return new JsonData(0, data, msg);
	}

	// 成功，传入数据,及状态码
	public static JsonData buildSuccess(Object data, int code) {
		return new JsonData(code, data, null);
	}

	// 成功，传入数据,及状态码,描述信息，页面大小，总数，当前页面
	public static JsonData buildSuccess( int code, Object data,String msg, int currentpage, int pagesize,
			int totalnum) {
		return new JsonData(code, data, msg, currentpage, pagesize, totalnum);
	}

	public Integer getCode() {
		return code;
	}

	public void setCode(Integer code) {
		this.code = code;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public int getCurrentpage() {
		return currentpage;
	}

	public void setCurrentpage(int currentpage) {
		this.currentpage = currentpage;
	}

	public int getPagesize() {
		return pagesize;
	}

	public void setPagesize(int pagesize) {
		this.pagesize = pagesize;
	}

	public int getTotalnum() {
		return totalnum;
	}

	public void setTotalnum(int totalnum) {
		this.totalnum = totalnum;
	}

	@Override
	public String toString() {
		return "JsonData [code=" + code + ", data=" + data + ", msg=" + msg + ", currentpage=" + currentpage
				+ ", pagesize=" + pagesize + ", totalnum=" + totalnum + "]";
	}

}
