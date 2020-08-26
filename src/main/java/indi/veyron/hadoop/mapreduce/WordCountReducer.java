package indi.veyron.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * map的输出到reduce端，是按照相同的key分发到一个reduce上去执行
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;

        Iterator<IntWritable> iterator = values.iterator();
        while(iterator.hasNext()){
            IntWritable value = iterator.next();
            count += value.get();
        }

        context.write(key,new IntWritable(count));
    }
}
