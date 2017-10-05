package Tool;

import java.security.MessageDigest;
import java.util.*;


public class Hasher {
	public  String byteArrayToHex(byte[] byteArray) {
		  char[] hexDigits = {'0','1','2','3','4','5','6','7','8','9', 'a','b','c','d','e','f' };
		  char[] resultCharArray =new char[byteArray.length * 2];
		  int index = 0;
		  for (byte b : byteArray) {
		     resultCharArray[index++] = hexDigits[b>>> 4 & 0xf];
		     resultCharArray[index++] = hexDigits[b& 0xf];
		  }
		  return new String(resultCharArray);
	}
    public  String stringSHA(String input) {  
    	   try {  
    	      MessageDigest messageDigest =MessageDigest.getInstance("SHA-256");      	  

    	      byte[] inputByteArray = input.getBytes();  

    	      messageDigest.update(inputByteArray);  

    	      byte[] resultByteArray = messageDigest.digest();  
    	      return new String(byteArrayToHex(resultByteArray));
    	  } catch (Exception e) {  
    	      return null;  
    	  }  
    	  
    }
    
    public static void main(String args[]) {
    	String str = new String("Hello World!");
    	String md5str = new Hasher().stringSHA(str);
    	System.out.println(md5str);
    	//System.out.println("32323"=="32323");
    	
    }
}