/*
 * DDL:
 * 1.�жϱ��Ƿ����
 * 2.������
 * 3.ɾ����
 * 4.���������ռ�
 * 
 * DML:
 * 5.��������
 * 6.��ȡ����
 * 7.ɾ������
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
			// 1.���÷���������Ϣ
			Configuration configuration = HBaseConfiguration.create();
			configuration.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
			// 2.���������������
			connection = ConnectionFactory.createConnection(configuration);
			// 3.����Admin����(���е�DDL��������ͨ��admin������в���)
			admin = connection.getAdmin();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// �жϱ��Ƿ���ڷ���
	public static boolean isTableExist(String tableName) throws IOException {
		return admin.tableExists(TableName.valueOf(tableName));
	}

	// ��������
	public static void createTable(String tableName, String... cfs) throws IOException {

		// 1.�ж��Ƿ���������Ϣ
		if (cfs.length <= 0) {
			System.out.println("������������Ϣ��");
			return;
		}

		// 2.�жϱ��Ƿ����
		boolean tableExist = isTableExist(tableName);
		if (tableExist) {
			System.out.println(tableName + "���Ѵ��ڣ�");
			return;
		}

		// 3.������������
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		
		// 4.ѭ���������
		for (String cf : cfs) {
			//��������������
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
			//�������
			hTableDescriptor.addFamily(hColumnDescriptor);
		}

		// 5.������
		admin.createTable(hTableDescriptor);
		
	}
	
	//ɾ������
	public static void dropTable(String tableName) throws IOException{
		//1.�жϱ��Ƿ����
		boolean tableExist = isTableExist(tableName);
		if(!tableExist){
			System.out.println(tableName+"�����ڣ�");
			return;
		}
		
		//2.disable��
		admin.disableTable(TableName.valueOf(tableName));
		
		//3.ɾ����
		admin.deleteTable(TableName.valueOf(tableName));	
	}
	
	// ���������ռ�
	public static void createNameSpace(String nameSpace){
		// ���������ռ���������ͨ�������ռ��������ľ�̬����create������һ��Builder�ڲ���Ķ���
		//ͨ���ö����build��������һ�������ռ�����������
		NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
		// ���������ռ�
		try {
			admin.createNamespace(namespaceDescriptor);
		}catch (NamespaceExistException e) {
			System.out.println(nameSpace+"�����ռ��Ѵ���");//�ֶ�catch�����ռ��Ѵ��ڵ��쳣
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// �ر���Դ����
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
	
	
	//��������
	public static void putData(String tableName,String rk,String cf,String cn,String value) throws IOException{
		//1.��ȡ�����
		Table table = connection.getTable(TableName.valueOf(tableName));
		//2.����put����
		Put put = new Put(Bytes.toBytes(rk));
		//3.put������Ӳ���
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
		put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("sex"), Bytes.toBytes("male"));
		put.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("name"), Bytes.toBytes("wangwu"));
		//4.��������
		table.put(put);
		//5.�ر���Դ
		table.close();
	}
	
	//��ȡ���ݣ�get��
	public static void getData(String tableName,String rk,String cf,String cn) throws IOException{
		//1.��ȡ�����
		Table table = connection.getTable(TableName.valueOf(tableName));
		//2.����get����
		Get get = new Get(Bytes.toBytes(rk));
		//3.get������Ӳ���
//		get.addFamily(Bytes.toBytes(cf));
		get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
		//4.��ȡ����
		Result results = table.get(get);
		Cell[] cells = results.rawCells();
		for (Cell cell : cells) {
			System.out.println("tableName��"+tableName+",rk:"+Bytes.toString(CellUtil.cloneRow(cell))+
		",cf:"+Bytes.toString(CellUtil.cloneFamily(cell))+
		",cn:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
			",value:"+Bytes.toString(CellUtil.cloneValue(cell)));
		}
		//5.�ر���Դ
		table.close();
	}
	
	//��ȡ���ݣ�scan��
	public static void scanData(String tableName) throws IOException{
		//1.��ȡ�����
		Table table = connection.getTable(TableName.valueOf(tableName));
		//2.��ȡscan����
		Scan scan = new Scan();
		//3.scan����
		ResultScanner scanners = table.getScanner(scan);
		for (Result scanner : scanners) {
			Cell[] cells = scanner.rawCells();
			for (Cell cell : cells) {
				System.out.println("tableName��"+tableName+",rk:"+Bytes.toString(CellUtil.cloneRow(cell))+
						",cf:"+Bytes.toString(CellUtil.cloneFamily(cell))+
						",cn:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
							",value:"+Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
		
		//4.�ر���Դ
		table.close();
	}
	
	
	//ɾ������
	public static void deleteData(String tableName,String rk,String cf,String cn) throws IOException{
		//1.��ȡ�����
		Table table = connection.getTable(TableName.valueOf(tableName));
		//2.����delete����
		Delete delete = new Delete(Bytes.toBytes(rk));//��
//		delete.addFamily(Bytes.toBytes(cf));//��
//		delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));//��
		delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));//��
		
		//3.ɾ������
		table.delete(delete);
		
		//4.�ر���Դ
		table.close();
	}
	

	public static void main(String[] args) throws IOException {
//		// 1.�жϱ��Ƿ���ڲ���
//		System.out.println(isTableExist("stu"));
//
//		// 2.���������
//		createTable("stu","info1","info2");
//		System.out.println(isTableExist("stu"));
//		
//		//3.ɾ�������
//		dropTable("stu");
//		System.out.println(isTableExist("stu"));
//		
//		//4.���������ռ����
//		createNameSpace("5055");
		
		//5.�������ݲ���
//		putData("stu", "1001", "info1", "name", "lisi");

		//6.��ȡ���ݣ�get������
//		getData("stu", "1001", "info1", "name");
		
//		//7.��ȡ���ݣ�scan������
//		scanData("stu");
		
		//8.ɾ�����ݲ��ԣ�delete��
		//deleteData("stu", "1001", "info1", "name");//��ֻ��ǰ�����������ã�ɾ��rk=1001���������ݣ����а汾��ɾ�������DeleteFamily
		//deleteData("stu1", "1001", "info", "name");//������������������ʱ��ɾ������info�µ��������ݣ����а汾��ɾ�������DeleteFamily
		//deleteData("stu1", "1001", "info", "name");//���ĸ�������������ʱ��ɾ��ĳһrk/cf/cn�����ݣ��ϰ汾�����ݻᲹλ�����Delete���˷�����������ݡ�թʬ���������к���ʹ��
		deleteData("stu1", "1001", "info", "name");//��addColumns:ɾ�����а汾���ݣ����DeleteColumn
		//9.ɾ�����ݲ��ԣ�deletes��
		
		// �ر���Դ
		close();

	}
}
