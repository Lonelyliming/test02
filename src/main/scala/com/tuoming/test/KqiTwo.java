package com.tuoming.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Author: Dawn
 * Date: 2022/7/26 11:01
 * Desc:
 * 1657509300|46000FEF975C|4|1778957|5923233|
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KqiTwo {
    public  SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");

    private String minTime;
    private String eci;
    private String type;
    private double upFlow;
    private double downFlow;

    private String hourDim;

    public KqiTwo(String minTime, String eci, String type, double upFlow, double downFlow) {
        this.minTime = minTime;
        this.eci = eci;
        this.type = type;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
    }


    public KqiTwo getInstance(String line){
        try {
            // 1657509300|46000FEF975C|4|1778957|5923233|
            String[] field = line.split("|");

            KqiTwo kqi = new KqiTwo();
            kqi.minTime = field[0];
            kqi.eci = field[1];
            kqi.upFlow = Double.valueOf(field[2]);
            kqi.downFlow = Double.valueOf(field[3]);

            kqi.hourDim = sdf.format(new Date(Long.valueOf(kqi.minTime)));

        }catch (Exception e){
            System.out.println("数据类型转换失败，数据为：" + line);
            e.printStackTrace();
        }
        return null;
    }

}
