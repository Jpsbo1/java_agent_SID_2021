package mongoToMysql;
import java.sql.*;
import java.util.LinkedList;

import org.bson.Document;

import com.google.gson.Gson;
import com.mongodb.*;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;


public class Mysql_demo {
	Connection conn = null;
	MongoClient mongoC;
	MongoDatabase db;
	MongoCollection<Document> coll;
	private LinkedList<SensorData> received = new LinkedList<SensorData>(); 
	private LinkedList<SensorData> listTmp = new LinkedList<SensorData>();
	private LinkedList<SensorData> listHum = new LinkedList<SensorData>();
	private LinkedList<SensorData> listLuz = new LinkedList<SensorData>();
	
	public void mongodbConnection() {
		
		mongoC= new MongoClient("localhost",27017);
		db= mongoC.getDatabase("sid2021");
		coll= db.getCollection("sid2021");

	}
	
	
	
	public void mysqlConnection(){
		String url = "jdbc:mysql://localhost:3306/";
		String dbName = "lab";
		String driver = "com.mysql.cj.jdbc.Driver";
		String userName = "root";
		String password = "";
		try {
			Class.forName(driver).newInstance();
			conn = DriverManager.getConnection(url + dbName, userName, password);
			PreparedStatement ps=conn.prepareStatement("INSERT into cultura(Nome_Cultura,ID_Zona) VALUES ('teste','12')");
			//Statement stmt = conn.createStatement();
			ps.executeUpdate();

		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	public void start() {
       //while true
		
		for (Document doc : coll.find().sort(Sorts.descending("_id"))){        
           Gson gson = new Gson();
           MongoLocalDocument mcd = gson.fromJson(doc.toJson(), MongoLocalDocument.class);
           received= (LinkedList<SensorData>) mcd.sensors;
           
           
            //System.out.println("[" + mcd.sensors + "] Reading added for sensor [" + mcd.Zona + ":" + mcd.Sensor + "]");
        }
	}
	
	
	
	
	
	
	//separar e meter nos respectivos array list para tratamento
	public void splitMeasurement() {
		for(SensorData i  :received ) {
			if(i.sensor.startsWith("T")) {
				
				
				listTmp.add(i);
			}
			if(i.sensor.startsWith("H")) {
				
				
				listHum.add(i);
			}
			if(i.sensor.startsWith("L")) {
				
				
				listLuz.add(i);
			}
			
		}
	}
	
	
	public boolean searchOutliers() {
		
	}
		
	//fazer os diferentes inserts para as tabelas
	public void insertMysql() {
		
	}
	
	public void checkMysql() {
		//
	}
	
	public static void main(String[] args) {
		Mysql_demo a = new Mysql_demo();
		a.mysqlConnection();
	}
}

