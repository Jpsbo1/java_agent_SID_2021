package mongoToMysql;
import java.math.RoundingMode;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

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
	public List<SensorData> received =  new ArrayList<>() ;
	public List<SensorData> outfree =  new ArrayList<>() ;

	
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
			//PreparedStatement ps=conn.prepareStatement("INSERT into cultura(Nome_Cultura,ID_Zona) VALUES ('teste','12')");
			//Statement stmt = conn.createStatement();
			//ps.executeUpdate();

		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	public void start() {
       //while true
		

	}
	
	
	
	
	public void receive_data() {
		for (Document doc : coll.find().sort(Sorts.descending("_id"))){        
	           Gson gson = new Gson();
	           MongoLocalDocument mcd = gson.fromJson(doc.toJson(), MongoLocalDocument.class);
	           System.out.println(mcd.sensors);
	           received= (List<SensorData>) mcd.sensors;
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
	
	
	public boolean searchOutliers(SensorData sens) {
		sens.data
	}
		
	
	public void insertMysqlAlert() throws SQLException {
		Statement ps=conn.createStatement();
		ResultSet r= ps.executeQuery("SELECT Hora FROM medicao ORDER BY Hora LIMIT 1");
		
		
	}
	
	
	
	
	//fazer os diferentes inserts para as tabelas
	public void insertMysqlMeasurement(SensorData data) throws SQLException {
			//inserir tb a hora de insersert na taab mysql
	        PreparedStatement ps =conn.prepareStatement("INSERT INTO medicao(Hora, Tipo, Outlier, Leitura) VALUES (" + data.hora + ", " + data.sensor + ", " + data.isOutlier + ", " + data.medicao + ");" );
	        ps.executeUpdate();
	   }
	
	
	public void checkMysqlDupli(SensorData sens)  {
		try {
			Statement ps=conn.createStatement();	
			ResultSet r = ps.executeQuery("SELECT Hora FROM medicao ORDER BY Hora LIMIT 1");
			String a;
			while( r.next()) {
				a= r.getTimestamp("Hora").toString();
				System.out.println(a);	
				
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}
	
	public void checkParameters() {
		try {
			Statement ps=conn.createStatement();
			ResultSet r= ps.executeQuery("SELECT * FROM parametro_cultura");
			int Limite_Inf_Temp;
			int Limite_Inf_Luz;
			int Limite_Inf_Hum;
			int Limite_Inf_Critico_Temp;
			int Limite_Inf_Critico_Luz;
			int Limite_Inf_Critico_Hum;
			int Limite_Sup_Temp;
			int Limite_Sup_Luz;
			int Limite_Sup_Hum;
			int Limite_Sup_Critico_Temp;
			int Limite_Sup_Critico_Luz;
			int Limite_Sup_Critico_Hum;
			int Zona;
			int id_cultura;
			while( r.next()) {
				Limite_Inf_Temp=r.getInt("Limite_Inf_Temp");
				Limite_Inf_Luz=r.getInt("Limite_Inf_Luz");
				Limite_Inf_Hum=r.getInt("Limite_Inf_Hum");
				Limite_Inf_Critico_Temp=r.getInt("Limite_Inf_Critico_Temp");
				Limite_Inf_Critico_Luz=r.getInt("Limite_Inf_Critico_Luz");
				Limite_Inf_Critico_Hum=r.getInt("Limite_Inf_Critico_Hum");
				Limite_Sup_Temp=r.getInt("Limite_Sup_Temp");
				Limite_Sup_Luz=r.getInt("Limite_Sup_Luz");
				Limite_Sup_Hum=r.getInt("Limite_Sup_Hum");
				Limite_Sup_Critico_Temp=r.getInt("Limite_Sup_Critico_Temp");
				Limite_Sup_Critico_Luz=r.getInt("Limite_Sup_Critico_Luz");
				Limite_Sup_Critico_Hum=r.getInt("Limite_Sup_Critico_Hum");
				Zona=r.getInt("Zona");
				id_cultura=r.getInt("ID_Cultura");
				for(SensorData i: outfree) {
					ResultSet r2= ps.executeQuery("SELECT * FROM medicao");
					int id_medicao=9999;
					if(r2.getTimestamp("Hora").equals(i.data)) {
						id_medicao=Integer.valueOf(r2.getInt("ID_Medicao"));
					}
					DecimalFormat decimal = new DecimalFormat("#.##");
					decimal.format(i.medicao);
					decimal.setRoundingMode(RoundingMode.DOWN);
					Timestamp timestamp = new Timestamp(System.currentTimeMillis());
					Statement stmt = conn.createStatement();
					String query = "NULL";
					if(i.sensor.startsWith("T")) {
						if(Limite_Sup_Temp<=Integer.valueOf(i.medicao) && Integer.valueOf(i.medicao)<=Limite_Sup_Critico_Temp) {
							if(Zona==(Integer.valueOf(i.sensor.charAt(i.sensor.length()-1)))) {
								query = "INSERT INTO alerta " + "VALUES("+timestamp+","+0+",'ALERTA SUPERIOR do sensor "+i.sensor+" da zona "+i.zona+".,"+id_cultura+","+id_medicao+","+decimal+")";
							}
						}
						else if(Integer.valueOf(i.medicao)>=Limite_Sup_Critico_Temp) {
							if(Zona==(Integer.valueOf(i.sensor.charAt(i.sensor.length()-1)))) {
								query = "INSERT INTO alerta " + "VALUES("+timestamp+","+1+",'ALERTA CRÍTICO SUPERIOR do sensor "+i.sensor+" da zona "+i.zona+".,"+id_cultura+","+id_medicao+","+decimal+")";
							}
						}
						else if(Limite_Inf_Temp>=Integer.valueOf(i.medicao) && Integer.valueOf(i.medicao)>=Limite_Inf_Critico_Temp) {
							if(Zona==(Integer.valueOf(i.sensor.charAt(i.sensor.length()-1)))) {
								query = "INSERT INTO alerta " + "VALUES("+timestamp+","+0+",'ALERTA INFERIOR do sensor "+i.sensor+" da zona "+i.zona+".,"+id_cultura+","+id_medicao+","+decimal+")";
							}
						}
						else if(Integer.valueOf(i.medicao)<=Limite_Inf_Critico_Temp) {
							if(Zona==(Integer.valueOf(i.sensor.charAt(i.sensor.length()-1)))) {
								query = "INSERT INTO alerta " + "VALUES("+timestamp+","+1+",'ALERTA CRÍTICO INFERIOR do sensor "+i.sensor+" da zona "+i.zona+".,"+id_cultura+","+id_medicao+","+decimal+")";
							}
						}
					}
					if(i.sensor.startsWith("H")) {
						if(Limite_Sup_Hum<=Integer.valueOf(i.medicao) && Integer.valueOf(i.medicao)<=Limite_Sup_Critico_Hum) {
							if(Zona==(Integer.valueOf(i.sensor.charAt(i.sensor.length()-1)))) {
								query = "INSERT INTO alerta " + "VALUES("+timestamp+","+0+",'ALERTA SUPERIOR do sensor "+i.sensor+" da zona "+i.zona+".,"+id_cultura+","+id_medicao+","+decimal+")";
							}
						}
						else if(Integer.valueOf(i.medicao)>=Limite_Sup_Critico_Hum) {
							if(Zona==(Integer.valueOf(i.sensor.charAt(i.sensor.length()-1)))) {
								query = "INSERT INTO alerta " + "VALUES("+timestamp+","+1+",'ALERTA CRÍTICO SUPERIOR do sensor "+i.sensor+" da zona "+i.zona+".,"+id_cultura+","+id_medicao+","+decimal+")";
							}
						}
						else if(Limite_Inf_Hum>=Integer.valueOf(i.medicao) && Integer.valueOf(i.medicao)>=Limite_Inf_Critico_Hum) {
							if(Zona==(Integer.valueOf(i.sensor.charAt(i.sensor.length()-1)))) {
								query = "INSERT INTO alerta " + "VALUES("+timestamp+","+0+",'ALERTA INFERIOR do sensor "+i.sensor+" da zona "+i.zona+".,"+id_cultura+","+id_medicao+","+decimal+")";
							}
							
						}
						else if(Integer.valueOf(i.medicao)<=Limite_Inf_Critico_Hum) {
							if(Zona==(Integer.valueOf(i.sensor.charAt(i.sensor.length()-1)))) {
								query = "INSERT INTO alerta " + "VALUES("+timestamp+","+1+",'ALERTA CRÍTICO INFERIOR do sensor "+i.sensor+" da zona "+i.zona+".,"+id_cultura+","+id_medicao+","+decimal+")";
							}
						}
					}
					if(i.sensor.startsWith("L")) {
						if(Limite_Sup_Luz<=Integer.valueOf(i.medicao) && Integer.valueOf(i.medicao)<=Limite_Sup_Critico_Luz) {
							if(Zona==(Integer.valueOf(i.sensor.charAt(i.sensor.length()-1)))) {
								query = "INSERT INTO alerta " + "VALUES("+timestamp+","+0+",'ALERTA SUPERIOR do sensor "+i.sensor+" da zona "+i.zona+".,"+id_cultura+","+id_medicao+","+decimal+")";
							}
						}
						else if(Integer.valueOf(i.medicao)>=Limite_Sup_Critico_Luz) {
							if(Zona==(Integer.valueOf(i.sensor.charAt(i.sensor.length()-1)))) {
								query = "INSERT INTO alerta " + "VALUES("+timestamp+","+1+",'ALERTA CRÍTICO SUPERIOR do sensor "+i.sensor+" da zona "+i.zona+".,"+id_cultura+","+id_medicao+","+decimal+")";
							}
						}
						else if(Limite_Inf_Luz>=Integer.valueOf(i.medicao) && Integer.valueOf(i.medicao)>=Limite_Inf_Critico_Luz) {
							if(Zona==(Integer.valueOf(i.sensor.charAt(i.sensor.length()-1)))) {
								query = "INSERT INTO alerta " + "VALUES("+timestamp+","+0+",'ALERTA INFERIOR do sensor "+i.sensor+" da zona "+i.zona+".,"+id_cultura+","+id_medicao+","+decimal+")";
							}
						}
						else if(Integer.valueOf(i.medicao)<=Limite_Inf_Critico_Luz) {
							if(Zona==(Integer.valueOf(i.sensor.charAt(i.sensor.length()-1)))) {
								query = "INSERT INTO alerta " + "VALUES("+timestamp+","+1+",'ALERTA CRÍTICO INFERIOR do sensor "+i.sensor+" da zona "+i.zona+".,"+id_cultura+","+id_medicao+","+decimal+")";
							}
						}
					}
					stmt.executeUpdate(query);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) throws SQLException {
		Mysql_demo a = new Mysql_demo();
		a.mysqlConnection();
		a.mongodbConnection();
		a.receive_data();
		a.checkMysqlDupli(a.received.get(0));
	}
}
