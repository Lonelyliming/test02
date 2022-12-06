package com.tuoming.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author: Dawn
 * Date: 2022/7/26 11:01
 * Desc: 202207110900|4600017079A002|0|0|0|0|0|0|
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Kqi {
    private String minTime;
    private String eci;
    private double videoMol;
    private double videoDen;
    private double phoneMol;
    private double phoneDen;
    private double payMol;
    private double payDen;

    private String hourDim;

    public Kqi getInstance(String line){
        try {
        // 202207110900|4600017079A002|0|0|0|0|0|0|
        String[] field = line.split("|");

        Kqi kqi = new Kqi();
        kqi.minTime = field[0];
        kqi.eci = field[1];
        kqi.videoMol = Double.valueOf(field[2]);
        kqi.videoDen = Double.valueOf(field[3]);
        kqi.phoneMol = Double.valueOf(field[4]);
        kqi.phoneDen = Double.valueOf(field[5]);
        kqi.payMol = Double.valueOf(field[6]);
        kqi.payDen = Double.valueOf(field[7]);

        String dayHour = kqi.minTime.substring(0, 10);
        kqi.hourDim = dayHour;

        }catch (Exception e){
            System.out.println("数据类型转换失败，数据为：" + line);
            e.printStackTrace();
        }
        return null;
    }


}
