import java.io.*;
import javax.swing.*;
import java.awt.image.*;
import java.util.*;

public class WriteYUV
{
	DataOutputStream dos;
	
	int width;
	int height;
	String type= "4:2:0";
	int oneFrameLength;
	
	public void startWriting(String filename)
	{
		try
		{
			dos = new DataOutputStream(new BufferedOutputStream(
					new FileOutputStream(filename)));

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	public void writeImage(BufferedImage bi)
	{
		int w = bi.getWidth();
		int h = bi.getHeight();

		LinkedList<Byte> uBuffer = new LinkedList<Byte>();
		LinkedList<Byte> vBuffer = new LinkedList<Byte>();
		try
		{
			boolean s = false;

			for (int j = 0; j < h; j++)
			{
				for (int i = 0; i < w; i++)
				{
					int color = bi.getRGB(i, j);

					int alpha = color >> 24 & 0xff;
					int red = color >> 16 & 0xff;
					int green = color >> 8 & 0xff;
					int blue = color & 0xff;

					//int y = (int) ((0.257 * red) + (0.504 * green) + (0.098 * blue) + 16);
					//int v = (int) ((0.439 * red) - (0.368 * green) - (0.071 * blue) + 128);
					//int u = (int) (-(0.148 * red) - (0.291 * green) + (0.439 * blue) + 128);
					int y = red;
					//~ if(!s)
					//~ {
					//~ uBuffer.addLast((byte)(u & 0xff));
					//~ vBuffer.addLast((byte)(v & 0xff));
					//~ }
					dos.write(y);

					s = !s;
				}
			}


			int sampah = oneFrameLength - width * height;

			for (int i = 0; i < sampah; i++)
			{
				dos.write(128);
			}


		} catch (Exception e)
		{
			e.printStackTrace();
		}

	}

	public void endWriting()
	{
		try
		{
			dos.close();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}

}