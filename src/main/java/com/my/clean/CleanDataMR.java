package com.my.clean;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 清洗数据和提取有效数据的MapReduce程序<br>
 * 读取原始数据,先清洗，再提取数据<Br>
 * */
public class CleanDataMR extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CleanDataMR(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "cleanData");
        String table1 = conf.get("before_table");
        String table2 = conf.get("clean_table");
        conf.set("hbase.zookeeper.quorum", "192.168.49.128:2181");
        TableMapReduceUtil.initTableMapperJob(table1, new Scan(), CDMapper.class, ImmutableBytesWritable.class,
                MapWritable.class, job);
        TableMapReduceUtil.initTableReducerJob(table2, CDReducer.class, job);
        job.waitForCompletion(true);
        return 0;
    }

    /**
     * 获取一行数据，就是一个网页，网页的相对路径为kye，其他信息为value被输入了。<br>
     * 1，先清洗数据 f:st不是2 就不要，<br>
     * 2，提取需要的字段<Br>
     *
     * 输出key:baseUrl
     * 输出value:需要提取的一堆数据，封装的MapWritable对象
     * */
    public static class CDMapper extends TableMapper<ImmutableBytesWritable, MapWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result result,
                           Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, MapWritable>.Context context)
                throws IOException, InterruptedException {
            //获取到当前页面的url  自身行键其实是 url的相对路径，我们要拿url的绝对路径，取f:bas
            byte[] baseUrl =
                    result.getValue(Bytes.toBytes("f"),Bytes.toBytes("bas"));
            //获取爬取到的正确数据 f:st
            byte[] st =
                    result.getValue(Bytes.toBytes("f"),Bytes.toBytes("st"));
            //转化f:st下的byte[]数组数据为int类型数据
            int status =
                    Bytes.toInt(st);

            //值为2的即为正确数据 在此基础进行操作
            //需要获取到的数据有：t标题  s评分 cnt内容 iln入链个数 oln出链个数   iln入链列表 oln出链列表
            if(status==2){
                //获取内容p:c
                byte[] content =
                        result.getValue(Bytes.toBytes("f"),Bytes.toBytes("cnt"));
                //获取到的为null,则重新置为空字符串，或者0字节数据
                if(content==null){
                    content = Bytes.toBytes("");
                }
                //获取标题p:t
                byte[] title =
                        result.getValue(Bytes.toBytes("p"),Bytes.toBytes("t"));
                //获取到的为null,则重新置为空字符串，或者0字节数据
                if( title == null){
                    title = Bytes.toBytes("");
                }
                //获得ol列族的Map数据
                NavigableMap<byte[], byte[]> olMap =
                        result.getFamilyMap(Bytes.toBytes("ol"));
                //统计列族ol的行数
                int olNum = olMap.size();
                //统计列族ol的链接列表,遍历ol列族
                //把数据封装为MapWritable对象，key=列名 value=值
                MapWritable mwo = new MapWritable();
                Set<byte[]> olk = olMap.keySet();
                for (byte[] kol : olk){
                    byte[] vol = olMap.get(kol);
                    mwo.put(new BytesWritable(kol),new BytesWritable(vol));
                }
                //获得il列族的Map数据
                NavigableMap<byte[],byte[]> ilMap =
                        result.getFamilyMap(Bytes.toBytes("il"));
                //统计列族il的行数
                int ilNum = ilMap.size();
                //统计列族il的链接列表
                MapWritable mwi = new MapWritable();
                Set<byte[]> ilk = ilMap.keySet();
                for (byte[] kil : ilk){
                    byte[] vil = ilMap.get(kil);
                    mwi.put(new BytesWritable(kil),new BytesWritable(vil));
                }
                //新建MapWritable类型对象，为了把每个字段以键值对类型继续传递给reduce，封装到MapWritable对象输出
                MapWritable mapWritable = new MapWritable();
                //key值为c   value值为 p:c
                mapWritable.put(
                        new BytesWritable(Bytes.toBytes("c")),
                        new BytesWritable(content));
                //key值为t   value值为 p:t
                mapWritable.put(
                        new BytesWritable(Bytes.toBytes("t")),
                        new BytesWritable(title));
                //key值为oln   value值为 出链接个数
                mapWritable.put(
                        new BytesWritable((Bytes.toBytes("oln"))),
                        new BytesWritable(Bytes.toBytes(olNum)));
                //key值为iln   value值为 入链接个数
                mapWritable.put(
                        new BytesWritable((Bytes.toBytes("iln"))),
                        new BytesWritable(Bytes.toBytes(ilNum)));
                //key值为ol_list   value值为 出链接列表
                mapWritable.put(
                        new BytesWritable(Bytes.toBytes("ol_list")),
                        mwo
                );
                //key值为il_list   value值为 入链接列表
                mapWritable.put(
                        new BytesWritable(Bytes.toBytes("il_list")),
                        mwi
                );
                //map输出结果 key为 baseUrl  value为上面整理好的map集合
                context.write(new ImmutableBytesWritable(baseUrl),mapWritable);
            }
        }
    }

    /**
     * 将清洗之后的数据保存到Hbase集群的clean_webpage表中
     * clean_webpage表需要预先创建
     *
     * create 'clean_webpage','page','il','ol'
     *
     * page列族用来存放 title，内容，出链接个数，入链接个数
     * il列族存放入链接情况 该列族下列名为 入链接url 值为入链接标签内容
     * ol列族存放入链接情况 该列族下列名为 出链接url 值为出链接标签内容
     * */
    public static class  CDReducer
            extends TableReducer<ImmutableBytesWritable, MapWritable, NullWritable>{
        /**
         * 将数据整理一下格式存储到clean_webpage表中
         * */
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
            //取得values中的第一个值，即mapper输出的Map容器，因为url不会重复即key不会重复，
            //此处也只有一个值(提示使用迭代器的next方法)
            MapWritable map =  values.iterator().next();

            //构建Put对象用来组织数据存入hbase，该对象的使用key作为行健
            Put p = new Put(key.get());

            //在Map容器中取出key为t的value值,将其添加到put中,列族是page,列名是t
            BytesWritable t = (BytesWritable)map.get(new BytesWritable(Bytes.toBytes("t")));
            p.addColumn(
                    Bytes.toBytes("page"), Bytes.toBytes("t"),t.getBytes()
            );
            //在Map容器中取出key为c的value值,将其添加到put中,列族是page,列名是c
            BytesWritable c = (BytesWritable) map.get(new BytesWritable(Bytes.toBytes("c")));
            p.addColumn(
                    Bytes.toBytes("page"),Bytes.toBytes("c"),c.getBytes()
            );
            //在Map容器中取出key为oln的value值,将其添加到put中,列族是page,列名是oln
            BytesWritable oln = (BytesWritable) map.get(new BytesWritable(Bytes.toBytes("oln")));
            p.addColumn(
                    Bytes.toBytes("page"),Bytes.toBytes("oln"),oln.getBytes()
            );

            //在Map容器中取出key为iln的value值,将其添加到put中,列族是page,列名是iln
            BytesWritable iln = (BytesWritable) map.get(new BytesWritable(Bytes.toBytes("iln")));
            p.addColumn(
                    Bytes.toBytes("page"),Bytes.toBytes("iln"), (iln.getBytes())
            );
            //在Map容器中取出key为ol_list的value值，此值的类型是MapWritable，代表了一整个出连接列表
            MapWritable olMap = (MapWritable) map.get(
                    new BytesWritable(Bytes.toBytes("ol_list")));

            //遍历olList,数据插入ol列族中，列名是olList中的key值，值是olList的value值
            Set<Writable> olk = olMap.keySet();
            for (Writable kol : olk){
                BytesWritable vol = (BytesWritable)olMap.get(kol);
                p.addColumn(
                        Bytes.toBytes("ol"),
                        ((BytesWritable)kol).getBytes(),vol.getBytes());
            }

            //在Map容器中取出key为il_list的value值，此值的类型是MapWritable，代表了一整个入连接列表
            MapWritable ilMap = (MapWritable) map.get(
                    new BytesWritable(Bytes.toBytes("il_list")));

            //遍历ilList,数据插入il列族中，列名是ilList中的key值，值是ilList的value值
            Set<Writable> ilk = ilMap.keySet();
            for (Writable kil : ilk){
                BytesWritable vil = (BytesWritable)olMap.get(kil);
                p.addColumn(
                        Bytes.toBytes("il"),
                        ((BytesWritable)kil).getBytes(),vil.getBytes());
            }
            //put拼完后，进行输出，Reducer输出的key为null，value是put
            context.write(NullWritable.get(),p);
        }
    }
}

