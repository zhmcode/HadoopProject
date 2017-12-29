package com.zhmcode.hadoop.mapjoin;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhm on 2017/12/29.
 */
public class OrderBean implements Writable {

	private int id;
	private String pid;
	private int num;
	private String pdName;
	private int tag;

	public int getTag() {
		return tag;
	}

	public void setTag(int tag) {
		this.tag = tag;
	}

	public OrderBean() {
	}

	public void setOrderBean(int id,String pid,int num,String pdName,int tag){
		this.id = id;
		this.pid = pid;
		this.num = num;
		this. pdName = pdName;
		this.tag = tag;
	}


	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}

	public String getPdName() {
		return pdName;
	}

	public void setPdName(String pdName) {
		this.pdName = pdName;
	}

	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(id);
		dataOutput.writeUTF(pid);
		dataOutput.writeInt(num);
		dataOutput.writeUTF(pdName);
		dataOutput.writeInt(tag);
	}

	public void readFields(DataInput dataInput) throws IOException {
		id = dataInput.readInt();
		pid = dataInput.readUTF();
		num = dataInput.readInt();
		pdName = dataInput.readUTF();
		tag = dataInput.readInt();
	}

	@Override
	public String toString() {
		return "OrderBean{" +
				"id=" + id +
				", pid='" + pid + '\'' +
				", num=" + num +
				", pdName='" + pdName + '\'' +
				'}';
	}
}
