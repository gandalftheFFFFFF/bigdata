
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Scanner;

public class XmlTest {

  public static void main(String args[]) {
	  
	  HashMap<Double, LinkedList<Data>> mapper;
	  mapper = new HashMap<>();
	  
	  
	  File file = new File("part-m-00000");

      try{
          Scanner scanner = new Scanner(file);
          
          while(scanner.hasNext()){
        	  String line = scanner.next();
        	  String[] arr = line.split(",");
        	  // 0time, 1type, 2id, 3x, 4y, 5angle, 6speed, 7pos, 8lane, 9edge
        	 
        	  Data tmpData = new Data();
        	  double id = Double.parseDouble(arr[2]);
        	 
        	  if(arr[1].equals("v")){
        		  tmpData.setTime(Double.parseDouble(arr[0]));
        		  tmpData.setX(Double.parseDouble(arr[3]));
        		  tmpData.setY(Double.parseDouble(arr[4]));
        		  tmpData.setSpeed(Double.parseDouble(arr[6]));
        		  tmpData.setAngle(Double.parseDouble(arr[5]));
        		  if(id==0.0){
//        			  System.out.print(tmpData.getTime()+" ");
 
        		  }
        	  }
        	  if(mapper.containsKey(id)){
        		  mapper.get(id).add(tmpData);
        	  }
        	  else{
        		  LinkedList<Data> list = new LinkedList<>();
        		  list.add(tmpData);
        		  mapper.put(id, list);
        	  }

        	  
        	  
          }
          scanner.close();
          
      }catch (FileNotFoundException e){

          e.printStackTrace();
      }
	 System.out.println("\n");
     LinkedList<Data> list = mapper.get(10.0);
     System.out.println("list size "+list.size());
     for(Data data : list){
    	 System.out.print(data.getTime()+" ");
     }
     
//     ListIterator<Data> itr = list.listIterator();
//     
//     while(itr.hasNext()){
//    	 Data data = itr.next();
//    	 System.out.print(data.getTime()+" ");
//     }
  }

}
class Data{
	private int id;
	private double time;
	private double x;
    private double y;
    private double angle;
    private double speed;
    private double pos;
    private String lane;
    private double slope;
    
    public void setId(int id){
    	this.id=id;
    }
    public int getId(){
    	return id;
    }
    public void setTime(double time){
    	this.time = time;
    }
    public double getTime(){
    	return time;
    }
    public void setX(double x){
    	this.x = x;
    }
    public double getX(){
    	return x;
    }
    public void setY(double y){
    	this.y = y;
    }
    public double getY(){
    	return y;
    }
    public void setAngle(double angle){
    	this.angle = angle;
    }
    public double getAngle(){
    	return angle;
    }
    public void setSpeed(double speed){
    	this.speed = speed;
    }
    public double getSpeed(){
    	return speed;
    }
}