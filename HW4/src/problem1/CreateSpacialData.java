package problem1;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class CreateSpacialData {
	public static int genInt(int min, int max){
		return new Random().nextInt(max-min+1) + min;
	}
	
	public static float genFloat(int min, int max){
		return new Random().nextFloat()*(max-min) + min;
	}
	
	public static void main(String[] args) throws FileNotFoundException, IOException{
		File points = new File("/home/hadoop/P1.csv");
		if(!points.exists())	points.createNewFile();
		BufferedWriter bw = new BufferedWriter(new FileWriter(points, false));
		for(int i=1;i<=10000000;i++){
			bw.write(Integer.toString(i)+','+genInt(0,10000)+','+genInt(0,10000));
			bw.newLine();
		}
		bw.close();
		
		//Rectangles
		File rectan = new File("/home/hadoop/R1.csv");
		if(!rectan.exists())	rectan.createNewFile();
		bw = new BufferedWriter(new FileWriter(rectan, false));
		for(int i=1;i<=2500000;i++){
			//id, top-left-point-x, top-left-point-y, height, width
			bw.write('r'+Integer.toString(i)+','+genFloat(0,10000-5)+','+genFloat(0,10000-20)+','+genFloat(1,20)+','+genFloat(1,5));
			bw.newLine();
		}
		bw.close();
	}
}
