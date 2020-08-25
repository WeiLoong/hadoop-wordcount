package indi.veyron.hadoop.hdfsyh;

import indi.veyron.hadoop.hdfs.Context;
import indi.veyron.hadoop.hdfs.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 使用HDFS API完成wordcount统计
 *
 * 功能拆解：
 * 1）读取HDFS上的文件 ==> HDFS API
 * 2）词频统计：对文件中的每一行数据都要进行业务处理（按照分隔符分割） ==> Mapper
 * 3）将处理结果缓存起来 ==> Context
 * 4）将结果输出到HDFS ==> HDFS API
 *
 * 代码重构：
 * 1）自定义配置文件重构代码
 * 2）反射创建自定义Mapper对象
 * 3）可插拔的业务逻辑处理————不区分大小写
 */
public class WCApp02 {
    public static void main(String[] args) throws Exception{

        //读取HDFS上的文件 ==> HDFS API
        Properties properties = ParamsUtils.getProperties();

        Path input = new Path(properties.getProperty(Constants.INPUT_PATH));
        //获取到要操作的HDFS文件系统
        FileSystem fs = FileSystem.get(new URI(properties.getProperty(Constants.HDFS_URI)),new Configuration(),"root");

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input,false);

        Context context = new Context();

        //通过反射创建Mapper对象
        Class<?> classmapper = Class.forName(properties.getProperty(Constants.MAPPER_CLASS));
        Mapper mapper = (Mapper) classmapper.getDeclaredConstructor().newInstance();
        //Mapper mapper = new WordCountMapper();

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
        Path output = new Path(properties.getProperty(Constants.OUTPUT_PATH));
        FSDataOutputStream out = fs.create(new Path(output,new Path(properties.getProperty(Constants.FILE_PATH))));

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
