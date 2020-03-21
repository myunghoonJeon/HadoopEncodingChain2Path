import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class Yuv {
	public void readAndCopy(long skipIndex,String input,String output) throws IOException{
		BufferedImage bi = new 
		byte[] buffer = new byte[1024];
		File localFile = new File(input);
		FileInputStream inputStream = new FileInputStream(localFile);//local case
//		int roofSize = Integer.parseInt(context.getConfiguration().get("nGOP"));
		long byteCount = 0;
		long wide = 3840;
		long height = 2160;
		long oneFrameLen = (long) (wide*height*1.5);
		long len = (long)(wide*height*(1.5));
/************************************************************************************/					
		FileOutputStream out = new FileOutputStream(output,false);
		inputStream.skip(skipIndex);
		int count = 1;
		long frameCount = 0;
		while(inputStream.read(buffer) != -1){
				out.write(buffer);
				byteCount += 1024;
				frameCount += 1024;
				if(frameCount == oneFrameLen){
					frameCount = 0;
					count++;
					System.out.println(count+" frame read success");
				}
				if(byteCount == len){
					System.out.println("wrighting.....");
					break;
				}
		}
		System.out.println("complete file write");
		inputStream.close();
		out.close();
		
	}
}
