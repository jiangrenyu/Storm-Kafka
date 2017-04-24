package com.bonc.utils;

public class CountMessageUtils {
	/**
	 * 
	 * @param str  
	 * @param count 初始是0
	 * @return
	 */
	public static int getCount(String str,int count) {

        int currentCount ;
//		if (str.equals("")) {
//			System.out.println("消息: " + str + "为空");
//			count++;
//		}

        currentCount = count+1;

		return currentCount;

	}
}
