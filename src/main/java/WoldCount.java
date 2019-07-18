import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WoldCount {
    /**
     * 自定义MyMapper类继承自Mapper
     */
    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
        //重写map方法
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[] splits = line.split(",");
            for (String split : splits) {
                //写出
                context.write(new Text(split), new LongWritable(1l));
            }
        }
    }
    /**
     * 自定义Myreduce类
     */
    public static class MyReduce extends Reducer<Text,LongWritable,Text,LongWritable> {
        //每一个reduce函数处理相同key的一组数据
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum=0;
            for (LongWritable value : values) {
                //遍历迭代器中的value，也就是相同key的一组value
                //将每个value相加
                sum=sum+=value.get();
            }
            //将结果数据输出，写出文件到磁盘
            context.write(key, new LongWritable(sum));

        }
    }

    /**
     * main程序入口
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取默认配置信息，如何想自定义修改一些参数，可以使用 conf.set()传入配置参数
        Configuration configuration = new Configuration();
        //获取job任务
        Job job = Job.getInstance(configuration, WoldCount.class.getSimpleName());
        //指定运行的jar包的类
        job.setJarByClass(WoldCount.class);
        //指定读取数据的路径
        //FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.setInputPaths(job, new Path("data/wordcount"));
        //指定map类
        job.setMapperClass(MyMapper.class);
        //指定map输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //指定reduce类型，keyout，valueout类型
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //指定输出路径
        FileOutputFormat.setOutputPath(job, new Path("output/wc"));
        //提交任务等待完成
        job.waitForCompletion(true);

    }
}
