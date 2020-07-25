package com.my.clean;

import java.io.IOException;

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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 构建倒置索引表
 * 新建表 invertindex
 * create 'invertindex','page'
 * 行健是 关键字  page列族下 放 url:rank##cnt
 * 为了方便对接页面
 * */
public class InvertIndexMR extends Configured implements Tool {

    public static void main(String[] args) {
        try {
            ToolRunner.run(new InvertIndexMR(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf,"BuildInvertIndex");
        job.setJarByClass(InvertIndexMR.class);
        TableMapReduceUtil.initTableMapperJob
                ("briup:clean_webpage",
                        new Scan(),
                        IIMapper.class,
                        ImmutableBytesWritable.class,
                        MapWritable.class,job);
        TableMapReduceUtil.initTableReducerJob("briup:invertindex",IIReducer.class,job);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
        return 0;
    }

    /**
     * 读取clean_webpage中数据<Br>
     * 输入key: rowkey行键，url
     * 输入value:行键以外的数据
     *
     * 输出key:页面的关键字
     * 输出value:该关键字对应的rank,url,c等值的MapWritable对象
     * */
    public static class IIMapper extends TableMapper<ImmutableBytesWritable,MapWritable>{
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //如果该行包含非空  page:key   page:rank，则为有效行，如果是有效行执行下面操作
            if(value.containsNonEmptyColumn(Bytes.toBytes("page"), Bytes.toBytes("key"))
                    && value.containsNonEmptyColumn(Bytes.toBytes("page"), Bytes.toBytes("rank"))) {
                //有效  则取 page:key
                byte[] keyword = value.getValue(Bytes.toBytes("page"), Bytes.toBytes("key"));
                //有效则取 page:rank
                byte[] rank = value.getValue(Bytes.toBytes("page"), Bytes.toBytes("rank"));
                //有效则取 page:c
                byte[] cnt = value.getValue(Bytes.toBytes("page"), Bytes.toBytes("c"));
                //如果page:c为空 则赋值空字符串
                if(cnt==null) {
                    cnt = Bytes.toBytes("");
                }
                //构建MapWritable进行存放三个值  rank   key   cnt
                MapWritable map = new MapWritable();
                map.put(new Text("url"), new Text(key.get()));
                map.put(new Text("rank"), new DoubleWritable(Bytes.toDouble(rank)));
                map.put(new Text("cnt"), new Text(cnt));
                //页面关键字去空格
                String kw = Bytes.toString(keyword).trim();
                //输出
                context.write(new ImmutableBytesWritable(kw.getBytes()),map);
            }
        }
    }
    /**
     *
     * */
    public static class IIReducer extends TableReducer<ImmutableBytesWritable,MapWritable,NullWritable>{
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            //获取页面关键字的字符串表现形式，要去空格
            String kw = new String(key.get()).trim();
            //如果 页面关键字字符串 长度不为0，则进行下面操作
            if(kw.length() != 0){
                //构建put,页面关键字为 rowkey行键
                Put put = new Put(Bytes.toBytes(kw));

                //遍历reduce的value值
                for (MapWritable map : values) {
                    //从 reduce 的输入value中遍历得到的MapWritable中获取url  (Text)
                    Text url = (Text) map.get(new Text("url"));
                    //从 reduce 的输入value中遍历得到的MapWritable中获取rank (DoubleWritable)
                    DoubleWritable rank = (DoubleWritable) map.get(new Text("rank"));
                    //从 reduce 的输入value中遍历得到的MapWritable中获取cnt	  (Text)
                    Text cnt = (Text) map.get(new Text("cnt"));
                    //把数据添加到put的列中
                    //page为列族，url为列，rank+cnt为value值
                    put.addColumn(Bytes.toBytes("page"),Bytes.toBytes(url.toString()),Bytes.toBytes(rank.get()));
                }
                //输出
                context.write(NullWritable.get(),put);
            }
        }
    }
}
