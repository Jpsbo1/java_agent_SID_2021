package mongoToMysql;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
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
import com.mysql.cj.protocol.x.SyncFlushDeflaterOutputStream;


public class Mysql_demo {
	Connection conn = null;
	MongoClient mongoC;
	MongoDatabase db;
	MongoCollection<Document> coll;
	public List<SensorData> received = new ArrayList<>();
	public List<SensorData> outfree = new ArrayList<>();
	private int calibrar=0;
	public void mongodbConnection() {

		mongoC = new MongoClient("localhost", 27017);
		db = mongoC.getDatabase("sid2021");
		coll = db.getCollection("sid2021");

	}

	public void mysqlConnection() {
		String url = "jdbc:mysql://localhost:3306/";
		String dbName = "lab";
		String driver = "com.mysql.cj.jdbc.Driver";
		String userName = "root";
		String password = "";
		try {
			Class.forName(driver).newInstance();
			conn = DriverManager.getConnection(url + dbName, userName, password);



		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void start() throws SQLException, InterruptedException {
		while(true) {
			receive_data();
			splitMeasurement();
			checkParameters();
			received.clear();
			outfree.clear();
			Thread.sleep(1000);
		}

	}

	public void receive_data() {
		
		for (Document doc : coll.find().sort(Sorts.descending("_id")).limit(1)) {
			Gson gson = new Gson();
			MongoLocalDocument mcd = gson.fromJson(doc.toJson(), MongoLocalDocument.class);
			
			for(SensorData a :mcd.sensors) {
				 received.add(a);
			}

			
		}

	}

	public void splitMeasurement() throws SQLException {
		for (SensorData i : received) {
			//System.out.println(i+"  -----------------------------------------------------------------------antes de enviar");
			if (i.sensor.startsWith("T")) {
				insertMysqlMeasurement(i);
			}
			if (i.sensor.startsWith("H")) {
				insertMysqlMeasurement(i);
			}
			if (i.sensor.startsWith("L")) {
				insertMysqlMeasurement(i);
			}
		}
	}

	public boolean searchOutliers(SensorData sens) throws SQLException {
        Statement ps = conn.createStatement();
        ResultSet r = ps.executeQuery("SELECT * FROM medicao ORDER BY Hora DESC LIMIT 4");
        BigDecimal average = new BigDecimal("0.00");
        BigDecimal total = new BigDecimal("0.00");
        BigDecimal one = new BigDecimal("1.00");
        BigDecimal oneTen = new BigDecimal("1.10");
        BigDecimal zeroNine = new BigDecimal("0.90");
        BigDecimal medicao = new BigDecimal(sens.medicao);
        BigDecimal averageTop = new BigDecimal("0.00");
        BigDecimal averageBottom = new BigDecimal("0.00");
        ArrayList<BigDecimal> lista = new ArrayList<BigDecimal>();
        while (r.next()) {
            lista.add(r.getBigDecimal("Leitura"));
        }
        for (BigDecimal i : lista) {
            average = average.add(i);
            total = total.add(one);
        }

        if (calibrar < 4) {
            calibrar++;
        } else {
            if (!average.equals(new BigDecimal("0.00")) && !total.equals(new BigDecimal("0.00"))) {
                average = average.divide(total, RoundingMode.DOWN);
                averageTop = average.multiply(oneTen);
                averageBottom = average.multiply(zeroNine);
            }

            if ((medicao.compareTo(averageTop) == -1 || medicao.compareTo(averageTop) == 0)
                    && ((medicao.compareTo(averageBottom) == 1)  || medicao.compareTo(averageBottom) == 0)) {
                return false;
            }

            if (average.equals(new BigDecimal("0.00")) && total.equals(new BigDecimal("0.00"))) {
                return false;
            }
            return true;
        }
        return false;
    }

	// fazer os diferentes inserts para as tabelas
	public void insertMysqlMeasurement(SensorData data) throws SQLException {
		// inserir tb a hora de insersert na taab mysql
		Timestamp a= new Timestamp(data.data.getTime());
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
        
        String med_trata;
        
        if(data.medicao.length()-1<6) {
        	String aux=data.medicao;
        	while(aux.length()-1<6) {
        		aux=aux.concat("0");
        	}
        	med_trata = aux;
        	
        }
        else {
        	 med_trata =data.medicao.substring(0, 6);
        }
      
		if (data.sensor.startsWith("T")) {
            if (searchOutliers(data)  ){// || checkMysqlDupli(data)
				System.out.println(data.sensor+"' , '"+ med_trata.substring(0, 6)+"'  "+data.sensor.charAt(data.sensor.length()-1));
				PreparedStatement ps = conn
						.prepareStatement("INSERT INTO medicao(Hora, Tipo, Outlier, Leitura, ID_Zona) VALUES (' "+ a+ " ' "
								+ ",' "+ data.sensor +"' , ' "+ 1+ " ' ,  ' "+ med_trata.substring(0, 6) +"'"+ 
								", ' "+data.sensor.charAt(data.sensor.length()-1) +"' );");
				
				ps.executeUpdate();
			} else {
				System.out.println("não e out     "+data.sensor+"' , '"+ med_trata.substring(0, 6) +"'  "+data.sensor.charAt(data.sensor.length()-1));
				PreparedStatement ps = conn
						.prepareStatement("INSERT INTO medicao(Hora, Tipo, Outlier, Leitura, ID_Zona) VALUES (' "+ a+ " ' "
								+ ",' "+ data.sensor +"' , ' "+ 0+ " ' ,  ' "+ med_trata.substring(0, 6) +"'"+ 
								", ' "+data.sensor.charAt(data.sensor.length()-1) +"' );");
				ps.executeUpdate();
				outfree.add(data);
			}
		} else {
			if (checkMysqlDupli(data)) {
				System.out.println(data.sensor+"' , '"+ med_trata.substring(0, 6) +"'  "+data.sensor.charAt(data.sensor.length()-1));
				a= new Timestamp(data.data.getTime());
				PreparedStatement ps = conn
						.prepareStatement("INSERT INTO medicao(Hora, Tipo, Outlier, Leitura, ID_Zona) VALUES (' "+ a+ " ' "
								+ ",' "+ data.sensor +"' , ' "+ 1+ " ' ,  ' "+ med_trata.substring(0, 6) +"'"+ 
								", ' "+data.sensor.charAt(data.sensor.length()-1) +"' );");
				ps.executeUpdate();
			} else {
				System.out.println(data.sensor+"' , '"+ med_trata.substring(0, 6) +"'  "+data.sensor.charAt(data.sensor.length()-1));
				a= new Timestamp(data.data.getTime());
				PreparedStatement ps = conn
						.prepareStatement("INSERT INTO medicao(Hora, Tipo, Outlier, Leitura, ID_Zona) VALUES (' "+ a+ " ' "
								+ ",' "+ data.sensor +"' , ' "+ 0+ " ' ,  ' "+ med_trata.substring(0, 6) +"'"+ 
								", ' "+data.sensor.charAt(data.sensor.length()-1) +"' );");
				ps.executeUpdate();

				outfree.add(data);
			}
		}
	}

	public boolean checkMysqlDupli(SensorData sens) throws SQLException {
        Statement ps = conn.createStatement();
        ResultSet r = ps.executeQuery("SELECT Hora FROM medicao ORDER BY Hora DESC LIMIT 2");
        String a;
        Timestamp b = new Timestamp(sens.data.getTime());
        while (r.next()) {
            a = r.getTimestamp("Hora").toString();
            if (b.toString().equals(a)) {
                return true;
            }
        }
        return false;
    }

	public void checkParameters() {
		try {
			int id_medicao = 9999;
			Statement e= conn.createStatement();
			ResultSet r2 = e.executeQuery("SELECT * FROM medicao");
			for (SensorData i : outfree) {

				//meter tudo em timeStamp
				while(r2.next()) {
					if (r2.getTimestamp("Hora").equals(i.data)) {
					id_medicao = Integer.valueOf(r2.getInt("ID_Medicao"));
						}
					}
				
			}
			r2.close();
			
			
			Statement ps = conn.createStatement();
			ResultSet r = ps.executeQuery("SELECT * FROM parametro_cultura");
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
			char Zona;
			int id_cultura;
			int Zon;
			while (r.next()) {
				Limite_Inf_Temp = r.getInt("Limite_Inf_Temp");
				Limite_Inf_Luz = r.getInt("Limite_Inf_Luz");
				Limite_Inf_Hum = r.getInt("Limite_Inf_Hum");
				Limite_Inf_Critico_Temp = r.getInt("Limite_Inf_Critico_Temp");
				Limite_Inf_Critico_Luz = r.getInt("Limite_Inf_Critico_Luz");
				Limite_Inf_Critico_Hum = r.getInt("Limite_Inf_Critico_Hum");
				Limite_Sup_Temp = r.getInt("Limite_Sup_Temp");
				Limite_Sup_Luz = r.getInt("Limite_Sup_Luz");
				Limite_Sup_Hum = r.getInt("Limite_Sup_Hum");
				Limite_Sup_Critico_Temp = r.getInt("Limite_Sup_Critico_Temp");
				Limite_Sup_Critico_Luz = r.getInt("Limite_Sup_Critico_Luz");
				Limite_Sup_Critico_Hum = r.getInt("Limite_Sup_Critico_Hum");
				Zon = r.getInt("ID_Zona") ;
				Zona= (char) (Zon+'0') ;
				id_cultura = r.getInt("ID_Cultura");
				for (SensorData i : outfree) {

					DecimalFormat decimal = new DecimalFormat(i.medicao);
					
					decimal.setRoundingMode(RoundingMode.DOWN);
					Timestamp timestamp = new Timestamp(System.currentTimeMillis());
					Statement stmt = conn.createStatement();
				//update nao query	
					String query = "NULL";
					
					if (i.sensor.startsWith("T")) {
						if (Limite_Sup_Temp <= Float.valueOf(i.medicao) && Float.valueOf(i.medicao) <= Limite_Sup_Critico_Temp) {
							System.out.println(Zona+"  "+i.zona.charAt(1));
							if (Zona ==i.zona.charAt(1) ) {
								PreparedStatement aler = conn.prepareStatement("INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) " + "VALUES('" + timestamp + "','" + 0
										+ "','ALERTA SUPERIOR do sensor " + i.sensor + " da zona " + i.zona + ".','"
										+ id_cultura + "','" + id_medicao + "','" + decimal + "')");
								aler.executeUpdate();
								System.out.println("escrever");
							}
						} else if (Float.valueOf(i.medicao) >= Limite_Sup_Critico_Temp) {
							System.out.println(Zona+"  "+i.zona.charAt(1));
							if (Zona==i.zona.charAt(1)) {
								PreparedStatement aler = conn.prepareStatement("INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) " + "VALUES('" + timestamp + "','" + 1
										+ "','ALERTA CRÍTICO SUPERIOR do sensor " + i.sensor + " da zona " + i.zona
										+ ".','" + id_cultura + "','" + id_medicao + "','" + decimal + "')");
								aler.executeUpdate();
								System.out.println("escrever2");
							}
						} else if (Limite_Inf_Temp >= Float.valueOf(i.medicao)
								&& Float.valueOf(i.medicao) >= Limite_Inf_Critico_Temp) {
							if (Zona==i.zona.charAt(1)) {
								PreparedStatement aler = conn.prepareStatement("INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) " + "VALUES('" + timestamp + "','" + 0
										+ "','ALERTA INFERIOR do sensor " + i.sensor + " da zona " + i.zona + ".','"
										+ id_cultura + "','" + id_medicao + "','" + decimal + "')");
								aler.executeUpdate();
							}
						} else if (Float.valueOf(i.medicao) <= Limite_Inf_Critico_Temp) {
							if (Zona==i.zona.charAt(1)) {
								PreparedStatement aler = conn.prepareStatement("INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) " + "VALUES('" + timestamp + "','" + 1
										+ "','ALERTA CRÍTICO INFERIOR do sensor " + i.sensor + " da zona " + i.zona
										+ ".','" + id_cultura + "','" + id_medicao + "','" + decimal + "')");
								aler.executeUpdate();
							}
						}
					}
					if (i.sensor.startsWith("H")) {
						if (Limite_Sup_Hum <= Float.valueOf(i.medicao)
								&& Float.valueOf(i.medicao) <= Limite_Sup_Critico_Hum) {
							if (Zona==i.zona.charAt(1)) {
								PreparedStatement aler = conn.prepareStatement("INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) " + "VALUES('" + timestamp + "','" + 0
										+ "','ALERTA SUPERIOR do sensor " + i.sensor + " da zona " + i.zona + ".','"
										+ id_cultura + "','" + id_medicao + "','" + decimal + "')");
								aler.executeUpdate();
							}
						} else if (Float.valueOf(i.medicao) >= Limite_Sup_Critico_Hum) {
							if (Zona ==i.zona.charAt(1)) {
								PreparedStatement aler = conn.prepareStatement("INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura )  " + "VALUES('" + timestamp + "','" + 1
										+ "','ALERTA CRÍTICO SUPERIOR do sensor " + i.sensor + " da zona " + i.zona
										+ ".','" + id_cultura + "','" + id_medicao + "','" + decimal + "')");
								aler.executeUpdate();
							}
						} else if (Limite_Inf_Hum >= Float.valueOf(i.medicao)
								&& Float.valueOf(i.medicao) >= Limite_Inf_Critico_Hum) {
							if (Zona ==i.zona.charAt(1)) {
								PreparedStatement aler = conn.prepareStatement("INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) " + "VALUES('" + timestamp + "','" + 0
										+ "','ALERTA INFERIOR do sensor " + i.sensor + " da zona " + i.zona + ".','"
										+ id_cultura + "','" + id_medicao + "','" + decimal + "')");
								aler.executeUpdate();
							}

						} else if (Float.valueOf(i.medicao) <= Limite_Inf_Critico_Hum) {
							if (Zona ==i.zona.charAt(1)) {
								PreparedStatement aler = conn.prepareStatement("INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) " + "VALUES('" + timestamp + "','" + 1
										+ "','ALERTA CRÍTICO INFERIOR do sensor " + i.sensor + " da zona " + i.zona
										+ ".','" + id_cultura + "','" + id_medicao + "','" + decimal + "')");
								aler.executeUpdate();
							}
						}
					}
					if (i.sensor.startsWith("L")) {
						if (Limite_Sup_Luz <= Integer.valueOf(i.medicao)
								&& Float.valueOf(i.medicao) <= Limite_Sup_Critico_Luz) {
							if (Zona ==i.zona.charAt(1)) {
								PreparedStatement aler = conn.prepareStatement("INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) " + "VALUES('" + timestamp + "','" + 0
										+ "','ALERTA SUPERIOR do sensor " + i.sensor + " da zona " + i.zona + ".','"
										+ id_cultura + "','" + id_medicao + "','" + decimal + "')'");
								aler.executeUpdate();
							}
						} else if (Float.valueOf(i.medicao) >= Limite_Sup_Critico_Luz) {
							if (Zona ==i.zona.charAt(1)) {
								PreparedStatement aler = conn.prepareStatement("INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura )  " + "VALUES('" + timestamp + "','" + 1
										+ "','ALERTA CRÍTICO SUPERIOR do sensor " + i.sensor + " da zona " + i.zona
										+ ".','" + id_cultura + "','" + id_medicao + "','" + decimal + "')");
								aler.executeUpdate();
							}
						} else if (Limite_Inf_Luz >= Float.valueOf(i.medicao)
								&& Float.valueOf(i.medicao) >= Limite_Inf_Critico_Luz) {
							if (Zona ==i.zona.charAt(1)) {
								PreparedStatement aler = conn.prepareStatement("INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) " + "VALUES('" + timestamp + "','" + 0
										+ "','ALERTA INFERIOR do sensor " + i.sensor + " da zona " + i.zona + ".','"
										+ id_cultura + "','" + id_medicao + "','" + decimal + "')");
								aler.executeUpdate();
							}
						} else if (Float.valueOf(i.medicao) <= Limite_Inf_Critico_Luz) {
							if (Zona ==i.zona.charAt(1)) {
								PreparedStatement aler = conn.prepareStatement("INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) VALUES('" + timestamp + "','" + 1
										+ "','ALERTA CRÍTICO INFERIOR do sensor " + i.sensor + " da zona " + i.zona
										+ ".','" + id_cultura + "','" + id_medicao + "','" + decimal + "')");
								aler.executeUpdate();
							}
						}
					}
					
					
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws SQLException {
		Mysql_demo demo = new Mysql_demo();
		demo.mongodbConnection();
		demo.mysqlConnection();

		try {
			demo.start();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
