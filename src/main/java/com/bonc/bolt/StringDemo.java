package com.bonc.bolt;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class StringDemo {
	public static void main(String args[]) throws Exception {
		deleteAndReplace("E://系统文件/开发测试/input01/f.txt","E://系统文件/开发测试/out02/","|",",","");
//		String str = "       ";
//		System.out.println(str.trim().length());
	}

	public static String deleteAndReplace(String inputPath, String destPath, String regex, String replaceRegex,
			String tmp) throws Exception {

		BufferedReader br = null;
		BufferedWriter bw = null;
		String outputPath = null;
		File inputFile = null;
		File outFile = null;
		String targetRegex = null;

		if (inputPath != null && destPath != null) {
			try {
				String separator = System.getProperty("file.separator");
				inputFile = new File(inputPath);
				outputPath = destPath.endsWith(separator) ? destPath : (destPath + separator);
				outFile = new File(outputPath + tmp + inputFile.getName());

				br = new BufferedReader(new FileReader(inputFile));
				bw = new BufferedWriter(new FileWriter(outFile));
				int deleteLine = 1;
				int lineNum = 0;
				String inputStr = null;
				String outStr = null;
				while ((inputStr = br.readLine()) != null) {
					lineNum++;
					// if (lineNum == deleteLine) {
					// continue;
					// }
//					if (inputStr.trim().isEmpty() || inputStr.length() == 0) {
//						continue;
//					}
					//System.out.println("是否是空：" + (inputStr == null));
					System.out.println("每一行长度：" + inputStr.trim().isEmpty()+"长度："+inputStr.length()+"去空后的长度："+inputStr.trim().length());
					outStr = inputStr.replace(regex, replaceRegex).trim();
					bw.write(outStr);
					bw.newLine();
					bw.flush();
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (br != null) {
					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				if (bw != null) {
					try {
						bw.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

			}

		} else {
			throw new Exception("处理文件无法找到输入路径：" + inputPath + "无法找到输出路径：" + destPath);
		}

		return outFile.getAbsolutePath();

	}
}
