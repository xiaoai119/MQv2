package com.mq.test;

/**
 * Created By xfj on 2020/8/10
 */

import java.util.*;
import java.util.stream.Collectors;

public class poj1050 {
    public static void main(String[] agrs) {
        Scanner sc = new Scanner(System.in);
        int arraySize;
        String str = sc.nextLine();
        arraySize = Integer.parseInt(str);
        List<String> collect = new ArrayList<String>();
        int[][] data = new int[arraySize][arraySize];
        while (sc.hasNextLine()) {
            str = sc.nextLine();
            if(str.trim().isEmpty()&&!sc.hasNextLine()){break;}
            String[] split = str.split(" ");
            collect.addAll(Arrays.stream(split).filter(s -> !s.equals("")).collect(Collectors.toList()));
            if(collect.size()==arraySize*arraySize){break;}
        }
        assert (collect.size() == arraySize * arraySize);
        for (int i = 0; i < collect.size(); i++) {
            int col = i / arraySize;
            int row = i % arraySize;
            data[col][row] = Integer.parseInt(collect.get(i));
        }
        Calculate calculate = new Calculate();
        System.out.println(calculate.solve(data,arraySize));
    }

     static class Calculate {
        public  int solve(int[][]data,int size){
            int max=Integer.MIN_VALUE;
            for(int index1=1;index1<=size;index1++){
                //index1表示选取矩阵的行数
                for(int rowStart=0;rowStart<size-index1+1;rowStart++){
                    int[] rowSum=new int[size];
                    int rowEnd=rowStart+index1-1;
                    for(int index2=rowStart;index2<=rowEnd;index2++){
                        for(int index3=0;index3<size;index3++){
                            rowSum[index3]+=data[index2][index3];
                        }
                    }
                    int maxValue = getMaxValue(rowSum);
                    if(max<maxValue){max=maxValue;}
                }
            }
            return max;
        }

        private  int getMaxValue(int[] rowSum) {
            int[]maxValue=new int[rowSum.length];
            maxValue[0]=rowSum[0];
            for(int i=1;i<rowSum.length;i++){
                maxValue[i]=Math.max(rowSum[i],maxValue[i-1]+rowSum[i]);
            }
            return Arrays.stream(maxValue).max().getAsInt();
        }
    }

}
