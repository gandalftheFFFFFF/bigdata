import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Scanner;

public class XmlTest {

  public static void main(String args[]) {
	  
	  HashMap<Integer, LinkedList<Data>> mapperV = new HashMap<>();
	  HashMap<Integer, LinkedList<Data>> mapperP = new HashMap<>();
	  
	  File file = new File("part-m-00000");

      try{
          Scanner scanner = new Scanner(file);
          
          while(scanner.hasNext()){
        	  String line = scanner.next();
        	  String[] arr = line.split(",");
        	  // 0time, 1type, 2id, 3x, 4y, 5angle, 6speed, 7pos, 8lane, 9edge
        	  Data tmpData = new Data();
        	  int id = Integer.parseInt(arr[2]);
        	  tmpData.setId(Integer.parseInt(arr[2]));
           	  tmpData.setTime(Double.parseDouble(arr[0]));
    		  tmpData.setX(Double.parseDouble(arr[3]));
    		  tmpData.setY(Double.parseDouble(arr[4]));
    		  tmpData.setAngle(Double.parseDouble(arr[5]));
    		  tmpData.setSpeed(Double.parseDouble(arr[6]));
    		  tmpData.setPos(Double.parseDouble(arr[7]));    		  
        	   
        	  if(arr[1].equals("v")){
        		  tmpData.setLane(arr[8]);
        		  if(mapperV.containsKey(id)){
            	      mapperV.get(id).add(tmpData);
            	  }
            	  else{
            		  LinkedList<Data> list = new LinkedList<>();
            		  list.add(tmpData);
            		  mapperV.put(id, list);
            	  }
        	  }
        	  
        	  if(arr[1].equals("p")){
        		  tmpData.setEdge(arr[9]);
        		  if(mapperP.containsKey(id)){
            	      mapperP.get(id).add(tmpData);
            	  }
            	  else{
            		  LinkedList<Data> list = new LinkedList<>();
            		  list.add(tmpData);
            		  mapperP.put(id, list);
            	  }
        	  }
        	  
        	  
        	  
        	  
        	  
          }
          scanner.close();
          
      }catch (FileNotFoundException e){

          e.printStackTrace();
      }
     double allData;
     int count;
     for(int id : mapperP.keySet()){
    	 LinkedList<Data> list = mapperP.get(id);
    	 double x1, y1, x2, y2;
    	 x1=0; y1=0;
    	 double high = 0;
    	 for(Data data : list){
    		 x2=data.getX();
    		 y2=data.getY();
    		 double dist = 0;
    		 if(x1!=0 && y1!=0) dist = distFrom(x1, y1, x2, y2);
//    		 allData+=dist;
    		 if(dist>high) {high=dist;}
    		 x1=x2;
    		 y1=y2;
    	 }
    	 if(high>50)System.out.println("ID: "+id+" - High "+high);
    	 
     }

     
     
     System.out.println("mapper v size "+mapperV.size());
     System.out.println("mapper p size "+mapperP.size());
     
  }
  
  public static double distFrom(double lat1, double lng1, double lat2, double lng2) {
	    double earthRadius = 6371000; //meters
	    double dLat = Math.toRadians(lat2-lat1);
	    double dLng = Math.toRadians(lng2-lng1);
	    double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
	               Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
	               Math.sin(dLng/2) * Math.sin(dLng/2);
	    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
	    double dist = earthRadius * c;

	    return dist;
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
    private String edge;
    
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
    public void setPos(double pos){
    	this.pos = pos;
    }
    public double getPos(){
    	return pos;
    }
    public void setLane(String lane){
    	this.lane = lane;
    }
    public String getLane(){
    	return lane;
    }
    public void setEdge(String edge){
    	this.edge = edge;
    }
    public String getEdge(){
    	return edge;
    }
}