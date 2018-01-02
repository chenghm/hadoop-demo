package com.cmeeto.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class JoinValueWritable implements Writable {

	private Text content;
	private byte tag;

	public Text getContent() {
		return content;
	}

	public void setContent(Text content) {
		this.content = content;
	}

	public byte getTag() {
		return tag;
	}

	public void setTag(byte tag) {
		this.tag = tag;
	}

	public JoinValueWritable() {
		this(new Text(), (byte) 0);
	}

	public JoinValueWritable(Text content, byte tag) {
		this.content = content;
		this.tag = tag;
	}

	public void write(DataOutput out) throws IOException {
		out.writeByte(tag);
		content.write(out);

	}

	public void readFields(DataInput in) throws IOException {
		tag = in.readByte();
		content.readFields(in);

	}

}
