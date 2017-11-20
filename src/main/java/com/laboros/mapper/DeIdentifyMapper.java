package com.laboros.mapper;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeIdentifyMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {

	final int[] deIdentifyColumns = { 2,3,5,7 };
	private byte[] key="abcdefgh12345678".getBytes();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		// key --0
		// value
		// --11111,bbb1,12/10/1950,1234567890,bbb1@xxx.com,1111111111,M,Diabetes,78

		final String dataSeperator = ",";// Move to properties
		final String iLine = value.toString();

		if (StringUtils.isNotEmpty(iLine)) {
			final String[] columns = StringUtils.splitPreserveAllTokens(iLine,
					dataSeperator);
			StringBuffer encryptedRow = new StringBuffer();
			String val = null;
			for (int currentColumn = 0; currentColumn < columns.length; currentColumn++) {
				boolean needToEncrypt = checkColumnNeedToEncrypt(currentColumn);
				if (needToEncrypt) {
					try {
						val = encrypt(columns[currentColumn]);
					} catch (InvalidKeyException e) {
						e.printStackTrace();
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					} catch (NoSuchPaddingException e) {
						e.printStackTrace();
					} catch (IllegalBlockSizeException e) {
						e.printStackTrace();
					} catch (BadPaddingException e) {
						e.printStackTrace();
					}
					encryptedRow.append(val);
				} else {
					encryptedRow.append(columns[currentColumn]);
				}
				encryptedRow.append(dataSeperator);
			}
			context.write(new Text(encryptedRow.toString()), NullWritable.get());

		}
	};

	private String encrypt(String column) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
		Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
		SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
		cipher.init(Cipher.ENCRYPT_MODE, secretKey);
		String encryptedString = Base64.encodeBase64String(cipher.doFinal(column.getBytes()));
		return encryptedString.trim();

	}

	private boolean checkColumnNeedToEncrypt(int iColIdx) 
	{
		for (int i = 0; i < deIdentifyColumns.length; i++) 
		{
			if((iColIdx+1)==deIdentifyColumns[i])
			{
				return Boolean.TRUE;
			}
		}
		return Boolean.FALSE;
	}
}