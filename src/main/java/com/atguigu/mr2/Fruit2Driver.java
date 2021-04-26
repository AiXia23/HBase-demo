/*
 * ��Hbase��ָ���ı���ж�ȡ���ݣ���������д��Hbase��ָ���ı��
 * 
 * Ҫ���ڱ������б���MR������Ҫ��resourcesĿ¼�´���һ��hbase-site.xml�ļ���
 * ����hbase-1.3.1/conf�¸��ļ������ݸ��ƽ���
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
		
		//1.��ȡjob
		Job job = Job.getInstance(configuration);
		
		//2.jar��mapper��reducer��
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
		
		//3.mapper�����
//		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
//		job.setMapOutputValueClass(PUT.class);
		
		//4.�������·��
		
		//5.�ύjob
		boolean result = job.waitForCompletion(true);
		
		return result?0:1;
	}
	
	public static void main(String[] args) {
		
		try {
//			Configuration configuration = new Configuration();
			//Ҫ���MR�ڱ������У�configuration�Ķ�����Ҫʹ��HBaseConfiguration.create()
			//��Ϊcreate�����е�addHbaseResources������HBase�������ļ�
			Configuration configuration = HBaseConfiguration.create();
			int run = ToolRunner.run(configuration, new Fruit2Driver(), args);
			System.exit(run);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
