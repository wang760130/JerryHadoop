package com.jerry.hadoop.mapreducer.access;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 10.5.10.207 - [23/Jul/2015:20:27:19 +0800] 'GET qy.58.com/1276372187655/' 0.055 200 8751 'http://sz.58.com/kefu/pn3/pve_5363_248/?postdate=20150721_20150724' 'Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)' '123.114.73.169'
 */
public class Access {
	
	private String nginxIp;
	private String datatime;
	private String method;
	private String request;
	private String time;
	private String state;
	private String size;
	private String httpReferer;
	private String userAgent;
	private String remoteIp;
	
	public static Access parser(String line) {
		Access access = null;
		Pattern pattern = Pattern.compile("(\\d+.\\d+.\\d+.\\d+) - \\[([\\s\\w+/:,-]+)\\] '(\\w+) (.*)' (\\d+.\\d+) (\\d+) (\\d+) '(.*)' '(.*)' '(\\d+.\\d+.\\d+.\\d+)'");
		
		if(line == null || "".equals(line))
			return null;
		
		line = line.replaceAll("(\\(.*\\))", "");
		line = line.replaceAll("åå", "app");
			
		Matcher matcher = pattern.matcher(line);
		if (matcher.find()) {
			int count = matcher.groupCount();
			if(count == 10) {
				access = new Access();
				access.setNginxIp(matcher.group(1));
				access.setDatatime(matcher.group(2));
				access.setMethod(matcher.group(3));
				access.setRequest(matcher.group(4));
				access.setTime(matcher.group(5));
				access.setState(matcher.group(6));
				access.setSize(matcher.group(7));
				access.setHttpReferer(matcher.group(8));
				access.setUserAgent(matcher.group(9));
				access.setRemoteIp(matcher.group(10));
				
			} else {
				return access;
			}
	    }
		return access;
	}
	
	public String getNginxIp() {
		return nginxIp;
	}
	public void setNginxIp(String nginxIp) {
		this.nginxIp = nginxIp;
	}
	public String getDatatime() {
		return datatime;
	}
	public void setDatatime(String datatime) {
		this.datatime = datatime;
	}
	public String getMethod() {
		return method;
	}
	public void setMethod(String method) {
		this.method = method;
	}
	public String getRequest() {
		return request;
	}
	public void setRequest(String request) {
		this.request = request;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getSize() {
		return size;
	}
	public void setSize(String size) {
		this.size = size;
	}
	public String getHttpReferer() {
		return httpReferer;
	}
	public void setHttpReferer(String httpReferer) {
		this.httpReferer = httpReferer;
	}
	public String getUserAgent() {
		return userAgent;
	}
	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}
	public String getRemoteIp() {
		return remoteIp;
	}
	public void setRemoteIp(String remoteIp) {
		this.remoteIp = remoteIp;
	}
	@Override
	public String toString() {
		return "Access [nginxIp=" + nginxIp + ", datatime=" + datatime
				+ ", method=" + method + ", request=" + request + ", time="
				+ time + ", state=" + state + ", size=" + size
				+ ", httpReferer=" + httpReferer + ", userAgent=" + userAgent
				+ ", remoteIp=" + remoteIp + "]";
	}

	public static void main(String[] args) {
//		String line = "10.5.10.207 - [02/Jun/2015:23:59:23 +0800] 'GET qy.58.com/23305793552902/' 0.095 200 8889 'https://www.baidu.com/link?url=SavnsKzfhPHZ0tZwcjoRblYYeOEOtOKHSCjYoEiUVwXb0kSycr74ta35jsvhwOwy&ie=utf-8&f=3&tn=95225935_hao_pg&wd=%E7%BD%91%E6%84%9F%E8%87%B3%E5%AF%9F&oq=%E7%BD%91%E6%84%9F&rsp=1&inputT=10813' 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36' '58.212.4.201'";
//		Access.parser(line);
		
		String str = "10.5.10.208 - [02/Jun/2015:01:47:37 +0800] 'GET qy.58.com/js/highlightmaphelp.js' 0.002 200 1379 'http://qy.58.com/mapdetail.html?lat=&name=Ã¥Â¨ÂÃ¦ÂµÂ·Ã¦ÂÂ®Ã¦ÂÂ¨Ã¨Â´Â¸Ã¦ÂÂÃ¦ÂÂÃ©ÂÂÃ¥ÂÂ¬Ã¥ÂÂ¸' 'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET4.0C; .NET4.0E)' '61.150.76.7'";
		str = str.replaceAll("åå", "app");
		str = str.replaceAll("(\\(.*\\))", "");
		Pattern pattern = Pattern.compile("(\\d+.\\d+.\\d+.\\d+) - \\[([\\s\\w+/:,-]+)\\] '(\\w+) (.*)' (\\d+.\\d+) (\\d+) (\\d+) '(.*)' '(.*)' '(\\d+.\\d+.\\d+.\\d+)'");
		
		Matcher matcher = pattern.matcher(str);
		if (matcher.find()) {
			int count = matcher.groupCount();
			System.out.println(count);
			for(int i = 1; i <= count ; i++) {
				System.out.println(matcher.group(i));
			}
		
	    }
		
	}
}
