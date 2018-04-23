package com.laboros.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF; 
import org.apache.hadoop.io.Text;
public class Lower extends UDF{
	public Text evaluate(Text text){
		if(text==null) return null;
		
		String str = text.toString();
		
		str = str != null ? str.toLowerCase() : "";
		
		return new Text(str);
	}

}