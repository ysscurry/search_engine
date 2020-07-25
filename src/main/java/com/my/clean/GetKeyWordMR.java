package com.my.clean;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

/**
 * 提取每个页面的关键字，主要思路，因为p列族下的t列数据不是很准确
 * 这里可以使用每个页面入链接标签中的内容作为每个页面的主题内容即为关键字
 * */
public class GetKeyWordMR  extends Configured implements Tool {
    public static void main(String[] args) throws  Exception {
        ToolRunner.run(new GetKeyWordMR(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration=getConf();
        configuration.set("hbase.zookeeper.quorum","192.168.49.128:2181");
        Job job=Job.getInstance(configuration);
        job.setJobName(GetKeyWordMR.class.getName());
        job.setJarByClass(GetKeyWordMR.class);
        TableMapReduceUtil.initTableMapperJob(
                Bytes.toBytes("briup:clean_webpage"),
                new Scan(),
                KeyWordMapper.class,
                Text.class,
                Text.class, job);
        //本次写出去以后 ，需要page列族下多一个key列,pageKey值
        TableMapReduceUtil.initTableReducerJob(
                "briup:clean_webpage", KeyWordReducer.class,job
        );
        job.waitForCompletion(true);
        return 0;

    }

    /**
     * 提取每个页面的关键字
     * 简单提取：网页的关键字通过入链的超链接标签中的字符内容 以及当前网页中 title 来确定
     * */
    public static  class KeyWordMapper extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //准备好接收输出key的对象
            Text outKey=new Text();
            //准备好接收输出key的对象
            Text outValue=new Text();
            //设置行健为输出的key
            outKey.set(key.get());
            //获取页面标题，即获取clean_table表中page列族 t列内容
            byte[] title = value.getValue("page".getBytes(), "t".getBytes());
            //转化成字符串
            String stitle = Bytes.toString(title);
            //用于接收a标签中的文本值
            byte[] fv = "".getBytes();
            //如果该内容不是空且长度大于0，则去入链接列表中，第一个有内容的value当做title
            if (stitle != null && stitle.length()>0 ){
                System.out.println("-------------------");
                //获取入链的列表
                NavigableMap<byte[], byte[]> ilMap = value.getFamilyMap("il".getBytes());
                //如果map的长度大于0，对其进行循环，获取第一个有文字值的作为当前页面主题
                if(ilMap.size()>0) {
                    Set<byte[]> vill = ilMap.keySet();
                    //遍历所有的入链内容，把第一个非空值作为 keyword
                    for (byte[] ki : vill) {//这里的ki就是入链接的url
                        byte[]  vi = ilMap.get(ki);
                        if (vi.length > 0 && vi!=null) {
                            //拿到第一个入链的 内容 作为基础值
                            fv = vi;
                            break;
                        }
                    }
                }
            }

            //最后为了保险，将 il中获取到的 主题 和 title进行拼接  xxxx,yyyy
            String allt = Bytes.toString(fv) + stitle;
            //设置value
            outValue.set(allt);
            //Mapper进行键值对输出
            context.write(outKey,outValue);

        }
    }

    /**
     * 输入的key：clean_webpage表中的行键
     * 输入的value：【title+a文本，title+a文本，title+a文本】
     *
     * 这些数据需要写到hbase表clean_webpage中page列族key列
     *
     * */

    public static class KeyWordReducer extends TableReducer<Text,Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //构建一个插入数据对象Put
            Put put = new Put(key.getBytes());
            //获取该页面的关键字 values中的有且只有一个的关键字字符串
            //输出关键字 到page列族 key列下
            for (Text value : values) {
                put.addColumn(Bytes.toBytes("page"),Bytes.toBytes("key"),value.getBytes());
                context.write(NullWritable.get(),put);
            }

        }
    }

}