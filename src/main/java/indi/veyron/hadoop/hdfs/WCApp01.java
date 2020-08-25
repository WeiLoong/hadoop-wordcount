package indi.veyron.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Set;

/**
 * 使用HDFS API完成wordcount统计
 *
 * 功能拆解：
 * 1）读取HDFS上的文件 ==> HDFS API
 * 2）词频统计：对文件中的每一行数据都要进行业务处理（按照分隔符分割） ==> Mapper
 * 3）将处理结果缓存起来 ==> Context
 * 4）将结果输出到HDFS ==> HDFS API
 */
public class WCApp01 {
    public static void main(String[] args) throws Exception{

        //读取HDFS上的文件 ==> HDFS API
        Path input = new Path("/hdfsapi/demo/hello.txt");
        //获取到要操作的HDFS文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop001:8020"),new Configuration(),"root");

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input,false);

        Context context = new Context();
        Mapper mapper = new WordCountMapper();

        while(iterator.hasNext()){

            LocatedFileStatus file = iterator.next();
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while((line = reader.readLine()) != null){

                //词频统计 ==> Mapper
                mapper.map(line,context);
            }
            in.close();
            reader.close();
        }

        //将处理结果缓存起来 Map
        Map<Object,Object> contextMap = context.getCacheMap();

        //将结果输出到HDFS ==> HDFS API
        Path output = new Path("/hdfsapi/output/");
        FSDataOutputStream out = fs.create(new Path(output,new Path("wc.out")));

        //将缓存中的内容输出到out中去
        Set<Map.Entry<Object,Object>> entries = contextMap.entrySet();
        for (Map.Entry<Object,Object> entry : entries){
            out.write((entry.getKey().toString() + "\t" + entry.getValue() + "\n").getBytes());
        }
        fs.close();
        out.close();

        System.out.println("HDFS API词频统计完成！");
    }
}
