package indi.veyron.hadoop.hdfs;

/**
 * 自定义Mapper
 */
public interface Mapper {

    /**
     *
     * @param line  读取到每一行的数据
     * @param context   缓存
     */
    public void map(String line,Context context);
}
