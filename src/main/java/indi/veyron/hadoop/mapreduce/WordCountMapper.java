package indi.veyron.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN:Map任务读数据key类型，offset，是每行数据起始位置的偏移量，Long
 * VALUEIN:Map任务读数据的value类型，其实就是一行行的字符串，String
 *
 * KEYOUT:Map方法自定义实现输出的key类型，String
 * VALUEOUT:Map方法自定义实现输出的value类型，Integer
 *
 * 词频统计：相同单词的次数
 * Java数据类型 ==> Hadoop自定义数据类型
 * Long ==> LongWritable
 * String ==> Text
 * Integer ==> IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //把value对应的指定的行数据按照指定的分隔符拆开
        String[] words = value.toString().split("\t");

        for (String word : words ){
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
