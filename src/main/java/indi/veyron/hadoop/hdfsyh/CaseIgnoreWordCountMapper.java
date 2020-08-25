package indi.veyron.hadoop.hdfsyh;

import indi.veyron.hadoop.hdfs.Context;
import indi.veyron.hadoop.hdfs.Mapper;

/**
 * 自定义wc实现类
 */
public class CaseIgnoreWordCountMapper implements Mapper {


    @Override
    public void map(String line, Context context) {

        String[] word = line.toLowerCase().split("\t");
        for (String words :word){
            Object value = context.get(words);
            //若没有出现过该单词，则赋值value：1
            if (value == null){
                context.write(words,1);
            }
            else {
                int v = Integer.parseInt(value.toString());
                //若出现重复单词，则value+1
                context.write(words,v+1);
            }
        }
    }
}
