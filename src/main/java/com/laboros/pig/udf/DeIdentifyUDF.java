package com.laboros.pig.udf;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;

public class DeIdentifyUDF extends EvalFunc<String> 
{
	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() < 2) {
			this.warn("invalid number of arguments to DEIDENTIFY",
					(Enum) PigWarning.UDF_WARNING_1);
			return null;
		}
		try {
			String plainText = (String) input.get(0);
			String encryptKey = (String) input.get(1);
			String str = "";
			try {
				str = this.encrypt(plainText, encryptKey.getBytes());
			} catch (NoSuchPaddingException e) {
				str = "NoSuchPaddingException";
				e.printStackTrace();
			} catch (IllegalBlockSizeException e) {
				str = "IllegalBlockSizeException";
				e.printStackTrace();
			} catch (BadPaddingException e) {
				str = "BadPaddingException";
				e.printStackTrace();
			} catch (InvalidKeyException e) {
				str = "InvalidKeyException";
				e.printStackTrace();
			} catch (NoSuchAlgorithmException e) {
				str = "NoSuchAlgorithmException";
				e.printStackTrace();
			}
			return str;
		} catch (NullPointerException npe) {
			this.warn(npe.toString(), (Enum) PigWarning.UDF_WARNING_2);
			return null;
		} catch (StringIndexOutOfBoundsException npe) {
			this.warn(npe.toString(), (Enum) PigWarning.UDF_WARNING_3);
			return null;
		} catch (ClassCastException e) {
			this.warn(e.toString(), (Enum) PigWarning.UDF_WARNING_4);
			return null;
		}
	}

	private String encrypt(String strToEncrypt, byte[] key) throws NoSuchAlgorithmException, NoSuchPaddingException,InvalidKeyException, 
																							IllegalBlockSizeException, BadPaddingException {
		Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
		SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
		cipher.init(1, secretKey);
		String encryptedString = Base64.encodeBase64String((byte[]) cipher.doFinal(strToEncrypt.getBytes()));
//		System.out.println("------------encryptedString" + encryptedString);
		return encryptedString.trim();
	}
}
