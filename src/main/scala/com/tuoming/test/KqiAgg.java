package com.tuoming.test;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Author: Dawn
 * Date: 2022/7/26 11:00
 * Desc:
 */
public class KqiAgg {
    public static void main(String[] args) throws Exception {
        String exeDayHour = "2022072610";
        String exeDay = exeDayHour.substring(0,8);
        String exeHour = exeDayHour.substring(8,10);

        String kq1Path = "day=" + exeDay + "/hour=" + exeHour;
        String kq1OnePath = "day=" + exeDay + "/hour=" + exeHour;
        String kq1TowPath = "day=" + exeDay + "/hour=" + exeHour;
        List<String> list = new ArrayList<>();
        List<String> list1 = new ArrayList<>();
        List<String> list2 = new ArrayList<>();

        readDataToList(kq1Path,list);
        readDataToList(kq1OnePath,list1);
        readDataToList(kq1TowPath,list2);



    }



    public static void readDataToList(String path, List<String> list) throws Exception {
        File file = new File(path);

        File[] files = file.listFiles();
        if (files != null && file.isDirectory()) {
            for (File eachFile : files) {
                if (eachFile.isFile()) {
                    List<String> lines = IOUtils.readLines(new FileInputStream(eachFile));
                    System.out.println("文件："+eachFile.getAbsolutePath() +",读取到:" + lines.size() +"条记录");
                    list.addAll(lines);
                } else {
                    readDataToList(eachFile.getAbsolutePath(), list);
                }
            }
        }
    }


}
