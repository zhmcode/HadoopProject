package com.zhmcode.hadoop.flowsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhm on 2017/12/29.
 */
public class FlowSortBean implements WritableComparable<FlowSortBean> {

	private long upFlow;
	private long dwFlow;
	private long sumFlow;

	public FlowSortBean() {
	}

	public void setFlowBean(long upFlow,long dwFlow){
		this.upFlow = upFlow;
		this.dwFlow = dwFlow;
		this.sumFlow = this.upFlow + this.dwFlow;
	}

	@Override
	public String toString() {
		return "FlowSortBean{" +
				"upFlow=" + upFlow +
				", dwFlow=" + dwFlow +
				", sumFlow=" + sumFlow +
				'}';
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDwFlow() {
		return dwFlow;
	}

	public void setDwFlow(long dwFlow) {
		this.dwFlow = dwFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(upFlow);
		dataOutput.writeLong(dwFlow);
		dataOutput.writeLong(sumFlow);
	}

	public void readFields(DataInput dataInput) throws IOException {
		upFlow = dataInput.readLong();
		dwFlow = dataInput.readLong();
		sumFlow = dataInput.readLong();
	}

	public int compareTo(FlowSortBean o) {
		return this.sumFlow>o.sumFlow?-1:1;
	}
}
