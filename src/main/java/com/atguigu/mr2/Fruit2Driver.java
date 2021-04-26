/*
 * 从Hbase的指定的表格中读取数据，并将数据写入Hbase的指定的表格
 * 
 * 要想在本地运行本此MR程序，需要在resources目录下创建一个hbase-site.xml文件，
 * 并将hbase-1.3.1/conf下该文件的内容复制进来
 */

package com.atguigu.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Fruit2Driver implements Tool{
	
	private Configuration configuration = null;

	public Configuration getConf() {
		return configuration;
	}

	public void setConf(Configuration conf) {
		configuration = conf;
	}

	public int run(String[] args) throws Exception {
		
		//1.获取job
		Job job = Job.getInstance(configuration);
		
		//2.jar、mapper、reducer类
		job.setJarByClass(Fruit2Driver.class);
		TableMapReduceUtil.initTableMapperJob("fruit", 
				new Scan(), 
				Fruit2Mapper.class, 
				ImmutableBytesWritable.class, 
				Put.class, 
				job);
		TableMapReduceUtil.initTableReducerJob("fruit2", 
				Fruit2Reducer.class, 
				job);
		
		//3.mapper输出类
//		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
//		job.setMapOutputValueClass(PUT.class);
		
		//4.输入输出路径
		
		//5.提交job
		boolean result = job.waitForCompletion(true);
		
		return result?0:1;
	}
	
	public static void main(String[] args) {
		
		try {
//			Configuration configuration = new Configuration();
			//要想此MR在本地运行，configuration的定义需要使用HBaseConfiguration.create()
			//因为create方法中的addHbaseResources加载了HBase的配置文件
			Configuration configuration = HBaseConfiguration.create();
			int run = ToolRunner.run(configuration, new Fruit2Driver(), args);
			System.exit(run);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
