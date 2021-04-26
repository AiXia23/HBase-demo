/*
 * DDL:
 * 1.判断表是否存在
 * 2.创建表
 * 3.删除表
 * 4.创建命名空间
 * 
 * DML:
 * 5.插入数据
 * 6.获取数据
 * 7.删除数据
 * 
 */

package com.atguigu.TestAPI;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class APITest {

	private static Connection connection = null;
	private static Admin admin = null;

	static {
		try {
			// 1.配置服务器的信息
			Configuration configuration = HBaseConfiguration.create();
			configuration.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
			// 2.与服务器创建连接
			connection = ConnectionFactory.createConnection(configuration);
			// 3.创建Admin对象(所有的DDL操作都是通过admin对象进行操作)
			admin = connection.getAdmin();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// 判断表是否存在方法
	public static boolean isTableExist(String tableName) throws IOException {
		return admin.tableExists(TableName.valueOf(tableName));
	}

	// 创建表方法
	public static void createTable(String tableName, String... cfs) throws IOException {

		// 1.判断是否传入列族信息
		if (cfs.length <= 0) {
			System.out.println("请输入列族信息！");
			return;
		}

		// 2.判断表是否存在
		boolean tableExist = isTableExist(tableName);
		if (tableExist) {
			System.out.println(tableName + "表已存在！");
			return;
		}

		// 3.创建表描述器
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		
		// 4.循坏添加列族
		for (String cf : cfs) {
			//创建列族描述器
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
			//添加列族
			hTableDescriptor.addFamily(hColumnDescriptor);
		}

		// 5.创建表
		admin.createTable(hTableDescriptor);
		
	}
	
	//删除表方法
	public static void dropTable(String tableName) throws IOException{
		//1.判断表是否存在
		boolean tableExist = isTableExist(tableName);
		if(!tableExist){
			System.out.println(tableName+"表不存在！");
			return;
		}
		
		//2.disable表
		admin.disableTable(TableName.valueOf(tableName));
		
		//3.删除表
		admin.deleteTable(TableName.valueOf(tableName));	
	}
	
	// 创建命名空间
	public static void createNameSpace(String nameSpace){
		// 创建命名空间描述器（通过命名空间描述器的静态方法create，创建一个Builder内部类的对象，
		//通过该对象的build方法创建一个命名空间描述器对象）
		NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
		// 创建命名空间
		try {
			admin.createNamespace(namespaceDescriptor);
		}catch (NamespaceExistException e) {
			System.out.println(nameSpace+"命名空间已存在");//手动catch命名空间已存在的异常
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// 关闭资源方法
	public static void close() {
		if (admin != null) {
			try {
				admin.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (connection != null) {
			try {
				connection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	//插入数据
	public static void putData(String tableName,String rk,String cf,String cn,String value) throws IOException{
		//1.获取表对象
		Table table = connection.getTable(TableName.valueOf(tableName));
		//2.创建put对象
		Put put = new Put(Bytes.toBytes(rk));
		//3.put对象添加参数
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
		put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("sex"), Bytes.toBytes("male"));
		put.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("name"), Bytes.toBytes("wangwu"));
		//4.插入数据
		table.put(put);
		//5.关闭资源
		table.close();
	}
	
	//获取数据（get）
	public static void getData(String tableName,String rk,String cf,String cn) throws IOException{
		//1.获取表对象
		Table table = connection.getTable(TableName.valueOf(tableName));
		//2.创建get对象
		Get get = new Get(Bytes.toBytes(rk));
		//3.get对象添加参数
//		get.addFamily(Bytes.toBytes(cf));
		get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
		//4.获取数据
		Result results = table.get(get);
		Cell[] cells = results.rawCells();
		for (Cell cell : cells) {
			System.out.println("tableName："+tableName+",rk:"+Bytes.toString(CellUtil.cloneRow(cell))+
		",cf:"+Bytes.toString(CellUtil.cloneFamily(cell))+
		",cn:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
			",value:"+Bytes.toString(CellUtil.cloneValue(cell)));
		}
		//5.关闭资源
		table.close();
	}
	
	//获取数据（scan）
	public static void scanData(String tableName) throws IOException{
		//1.获取表对象
		Table table = connection.getTable(TableName.valueOf(tableName));
		//2.获取scan对象
		Scan scan = new Scan();
		//3.scan数据
		ResultScanner scanners = table.getScanner(scan);
		for (Result scanner : scanners) {
			Cell[] cells = scanner.rawCells();
			for (Cell cell : cells) {
				System.out.println("tableName："+tableName+",rk:"+Bytes.toString(CellUtil.cloneRow(cell))+
						",cf:"+Bytes.toString(CellUtil.cloneFamily(cell))+
						",cn:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
							",value:"+Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
		
		//4.关闭资源
		table.close();
	}
	
	
	//删除数据
	public static void deleteData(String tableName,String rk,String cf,String cn) throws IOException{
		//1.获取表对象
		Table table = connection.getTable(TableName.valueOf(tableName));
		//2.创建delete对象
		Delete delete = new Delete(Bytes.toBytes(rk));//①
//		delete.addFamily(Bytes.toBytes(cf));//②
//		delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));//③
		delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));//④
		
		//3.删除数据
		table.delete(delete);
		
		//4.关闭资源
		table.close();
	}
	

	public static void main(String[] args) throws IOException {
//		// 1.判断表是否存在测试
//		System.out.println(isTableExist("stu"));
//
//		// 2.创建表测试
//		createTable("stu","info1","info2");
//		System.out.println(isTableExist("stu"));
//		
//		//3.删除表测试
//		dropTable("stu");
//		System.out.println(isTableExist("stu"));
//		
//		//4.创建命名空间测试
//		createNameSpace("5055");
		
		//5.插入数据测试
//		putData("stu", "1001", "info1", "name", "lisi");

		//6.获取数据（get）测试
//		getData("stu", "1001", "info1", "name");
		
//		//7.获取数据（scan）测试
//		scanData("stu");
		
		//8.删除数据测试（delete）
		//deleteData("stu", "1001", "info1", "name");//①只有前两个参数有用：删除rk=1001的所有数据，所有版本都删除，标记DeleteFamily
		//deleteData("stu1", "1001", "info", "name");//②三个参数都起作用时：删除列族info下的所有数据，所有版本都删除，标记DeleteFamily
		//deleteData("stu1", "1001", "info", "name");//③四个参数都起作用时：删除某一rk/cf/cn的数据，老版本的数据会补位，标记Delete。此方法易造成数据“诈尸”，生产中很少使用
		deleteData("stu1", "1001", "info", "name");//④addColumns:删除所有版本数据，标记DeleteColumn
		//9.删除数据测试（deletes）
		
		// 关闭资源
		close();

	}
}
