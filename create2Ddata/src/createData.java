import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public class createData {
	public static float create(int min, int max){
		return new Random().nextFloat()*(max - min)+min;
	}
	
	public static void main(String[] args) throws IOException{
		File data = new File("/home/hadoop/data2.csv");
		if(!data.exists())	data.createNewFile();
		BufferedWriter bw = new BufferedWriter(new FileWriter(data, false));
		for(int i=0;i<30;i++){
			bw.write(Integer.toString(i)+','+Float.toString(create(0,100))+','+Float.toString(create(0,100)));
			bw.newLine();
		}
		bw.close();
		
		Scanner in = new Scanner(System.in);
		int k = in.nextInt();
		File center = new File("/home/hadoop/center.csv");
		bw = new BufferedWriter(new FileWriter(center, false));
		if(!center.exists())	data.createNewFile();
		for(int i=1;i<=k;i++){
			bw.write(Integer.toString(i)+','+Float.toString(create(0,100))+','+Float.toString(create(0,100)));
			bw.newLine();
		}
		bw.close();
		
	}
}
