package mongoToMysql;

import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import mongoToMysql.settings.AppSettings;
import mongoToMysql.settings.MongoSettings;
import mongoToMysql.settings.SqlSettings;
import org.bson.Document;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class Mysql_demo {
    private Connection conn = null;
    private MongoClient mongoC;
    private MongoDatabase db;
    private MongoCollection<Document> coll;
    public List<SensorData> received = new ArrayList<>();
    public List<SensorData> outfree = new ArrayList<>();
    private int calibrar = 0;
    ArrayList<Pair<Integer, Timestamp>> lastAlerts = new ArrayList<Pair<Integer, Timestamp>>();
    ArrayList<Pair<Integer, Integer>> timeouts = new ArrayList<Pair<Integer, Integer>>();

    public void mongodbConnection(String ip, int port, String database, String collection) {
 
        mongoC = new MongoClient(ip, port);
        db = mongoC.getDatabase(database);
        coll = db.getCollection(collection);
 
    }
 
    public void mysqlConnection(String ip, int port, String db, String username, String password) {
        String url = "jdbc:mysql://" + ip + ":" + port + "/";
        String dbName = db;
        String driver = "com.mysql.cj.jdbc.Driver";
        String userName = username;
        try {
            Class.forName(driver).newInstance();
            conn = DriverManager.getConnection(url + dbName, userName, password);
 
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
 
    public void start() throws SQLException, InterruptedException {
        while (true) {
            // long t = System.currentTimeMillis();
            receive_data();
            splitMeasurement();
 
            received.clear();
            outfree.clear();
            // System.out.println(System.currentTimeMillis()-t);
            Thread.sleep(1000);
        }
 
    }
 
    public void receive_data() {
 
        for (Document doc : coll.find().sort(Sorts.descending("_id")).limit(1)) {
            Gson gson = new Gson();
            MongoLocalDocument mcd = gson.fromJson(doc.toJson(), MongoLocalDocument.class);
 
            for (SensorData a : mcd.sensors) {
                received.add(a);
            }
 
        }
 
    }
 
    public void splitMeasurement() throws SQLException {
        for (SensorData i : received) {
            // System.out.println(i+"
            // -----------------------------------------------------------------------antes
            // de enviar");
            if (i.sensor.startsWith("T")) {
                insertMysqlMeasurement(i);
                checkParameters(i);
            }
            if (i.sensor.startsWith("H")) {
                insertMysqlMeasurement(i);
                System.out.println(i);
                checkParameters(i);
            }
            if (i.sensor.startsWith("L")) {
                insertMysqlMeasurement(i);
                checkParameters(i);
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
            if (r.getInt("ID_Zona") == Integer.valueOf(sens.zona.split("")[1])
                    && r.getString("Tipo").equals(sens.sensor.split("")[0])) {
                lista.add(r.getBigDecimal("Leitura"));
            }
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
                    && ((medicao.compareTo(averageBottom) == 1) || medicao.compareTo(averageBottom) == 0)) {
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
 
        Timestamp a = new Timestamp(data.data.getTime());
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
 
        String med_trata;
 
        if (data.medicao.length() - 1 < 6) {
            String aux = data.medicao;
            while (aux.length() - 1 < 6) {
                aux = aux.concat("0");
            }
            med_trata = aux;
 
        } else {
            med_trata = data.medicao.substring(0, 6);
        }
 
        if (data.sensor.startsWith("T")) {
            if (searchOutliers(data) || checkMysqlDupli(data)) {
                // System.out.println(data.sensor+"' , '"+ med_trata.substring(0, 6)+"'
                // "+data.sensor.charAt(data.sensor.length()-1));
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO medicao(Hora, Tipo, Outlier, Leitura, ID_Zona) VALUES (' " + a + " ' " + ",'"
                                + data.sensor.charAt(0) + "' , ' " + 1 + " ' ,  ' " + med_trata.substring(0, 6) + "'"
                                + ", ' " + data.sensor.charAt(data.sensor.length() - 1) + "' );");
 
                ps.executeUpdate();
 
            } else {
                // System.out.println("nÃ£o e out "+data.sensor+"' , '"+ med_trata.substring(0,
                // 6) +"' "+data.sensor.charAt(data.sensor.length()-1));
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO medicao(Hora, Tipo, Outlier, Leitura, ID_Zona) VALUES (' " + a + " ' " + ",'"
                                + data.sensor.charAt(0) + "' , ' " + 0 + " ' ,  ' " + med_trata.substring(0, 6) + "'"
                                + ", ' " + data.sensor.charAt(data.sensor.length() - 1) + "' );");
                ps.executeUpdate();
                outfree.add(data);
            }
        } else {
            if (checkMysqlDupli(data) || Float.valueOf(med_trata) < 0) {
                // System.out.println(data.sensor+"' , '"+ med_trata.substring(0, 6) +"'
                // "+data.sensor.charAt(data.sensor.length()-1));
                a = new Timestamp(data.data.getTime());
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO medicao(Hora, Tipo, Outlier, Leitura, ID_Zona) VALUES (' " + a + " ' " + ",'"
                                + data.sensor.charAt(0) + "' , ' " + 1 + " ' ,  ' " + med_trata.substring(0, 6) + "'"
                                + ", ' " + data.sensor.charAt(data.sensor.length() - 1) + "' );");
                ps.executeUpdate();
            } else {
                // System.out.println(data.sensor+"' , '"+ med_trata.substring(0, 6) +"'
                // "+data.sensor.charAt(data.sensor.length()-1));
                a = new Timestamp(data.data.getTime());
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO medicao(Hora, Tipo, Outlier, Leitura, ID_Zona) VALUES (' " + a + " ' " + ",'"
                                + data.sensor.charAt(0) + "' , ' " + 0 + " ' ,  ' " + med_trata.substring(0, 6) + "'"
                                + ", ' " + data.sensor.charAt(data.sensor.length() - 1) + "' );");
                ps.executeUpdate();
 
                outfree.add(data);
            }
        }
    }
 
    public boolean checkMysqlDupli(SensorData sens) throws SQLException {
        // verificar a hora da medição e o tipo e a zona e se houver igual não insere
        Statement ps = conn.createStatement();
        ResultSet r = ps.executeQuery("SELECT * FROM medicao ORDER BY Hora DESC LIMIT 5");
        String a;
        Timestamp b = new Timestamp(sens.data.getTime());
        while (r.next()) {
            a = r.getTimestamp("Hora").toString();
            if (b.toString().equals(a) && r.getString("Tipo").equals(sens.sensor)
                    && r.getInt("ID_Zona") == Integer.valueOf(sens.zona.split("")[1])) {
                return true;
            }
        }
        return false;
    }
 
    public void checkParameters(SensorData sens) {
        try {
            int id_medicao = 9999;
            Statement e = conn.createStatement();
            ResultSet r2 = e.executeQuery("SELECT * FROM medicao");
            Timestamp a = null;
 
            for (SensorData i : outfree) {
                while (r2.next()) {
                    a = new Timestamp(r2.getTimestamp("Hora").getTime());
                    Timestamp b = new Timestamp(i.data.getTime());
                    if (a.equals(b) && (r2.getInt("ID_Zona") == Integer.valueOf(sens.zona.split("")[1])
                            && r2.getString("Tipo").equals(sens.sensor.split("")[0]))) {
                        id_medicao = Integer.valueOf(r2.getInt("ID_Medicao"));
                        // System.out.println("Entrou");
                    }
                }
 
            }
            r2.close();
 
            // cultura....--- timeout
            // tuplos ou lista de acc
 
            Statement ps2 = conn.createStatement();
            ResultSet r3 = ps2.executeQuery("SELECT * FROM cultura");
            int timeOut;
            int ID_cultura2;// id cultura da tabela cultura
            boolean alreadyExists = false;
            while (r3.next()) {
                timeOut = r3.getInt("Time-out_Avisos");
                ID_cultura2 = r3.getInt("ID_Cultura");
                for (Pair<Integer, Integer> id : timeouts) {
                    if (id.first == ID_cultura2) {
                        id.second = timeOut;
                        alreadyExists = true;
                    }
                }
                if(!alreadyExists) {
                    Pair<Integer, Integer> t_o = new Pair<Integer, Integer>(ID_cultura2, timeOut);
                    timeouts.add(t_o);
                }
            }
 
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
            int id_cultura;// id cultura da tabela parametro_cultura
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
                Zon = r.getInt("ID_Zona");
                Zona = (char) (Zon + '0');
                id_cultura = r.getInt("ID_Cultura");
                System.out.println(hasTimeoutPassed(lastAlerts, timeouts, id_cultura)+ "         "+!hasAlertFromCult(id_cultura, lastAlerts)+ "         "+timeouts.isEmpty());
                if ((hasTimeoutPassed(lastAlerts, timeouts, id_cultura)
                		|| !hasAlertFromCult(id_cultura, lastAlerts)) || timeouts.isEmpty()) {
                    for (SensorData i : outfree) {
                        BigDecimal decimal = new BigDecimal(i.medicao);
                        // ---------------------------------------------------------------------------------------------------------------------------
                        Statement stmt = conn.createStatement();
                        String query = "NULL";
                        System.out.println("passou o if");
                        if (i.equals(sens)) {
                            boolean foundAlert = false;
                            if (i.sensor.startsWith("T")) {
                                if (Limite_Sup_Temp <= Float.valueOf(i.medicao)
                                        && Float.valueOf(i.medicao) <= Limite_Sup_Critico_Temp) {
                                    // System.out.println( i.sensor.startsWith("T") +" "+ Zona+"
                                    // "+i.zona.charAt(1));
                                    if (Zona == i.zona.charAt(1)) {
                                        PreparedStatement aler = conn.prepareStatement(
                                                "INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) "
                                                        + "VALUES('" + a + "','" + 0 + "','ALERTA SUPERIOR do sensor "
                                                        + i.sensor + " da zona " + i.zona + ".','" + id_cultura + "','"
                                                        + id_medicao + "','" + decimal + "')");
                                        aler.executeUpdate();
                                        foundAlert = true;
                                        // System.out.println("escrever");
                                    }
                                } else if (Float.valueOf(i.medicao) >= Limite_Sup_Critico_Temp) {
                                    // System.out.println( i.sensor.startsWith("T") +" "+ Zona+"
                                    // "+i.zona.charAt(1));
                                    if (Zona == i.zona.charAt(1)) {
                                        PreparedStatement aler = conn.prepareStatement(
                                                "INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) "
                                                        + "VALUES('" + a + "','" + 1
                                                        + "','ALERTA CRÍTICO SUPERIOR do sensor " + i.sensor
                                                        + " da zona " + i.zona + ".','" + id_cultura + "','"
                                                        + id_medicao + "','" + decimal + "')");
                                        aler.executeUpdate();
                                        foundAlert = true;
 
                                        // current time
                                        // System.out.println("escrever2");
                                    }
                                } else if (Limite_Inf_Temp >= Float.valueOf(i.medicao)
                                        && Float.valueOf(i.medicao) >= Limite_Inf_Critico_Temp) {
                                    // System.out.println( i.sensor.startsWith("T") +" "+ Zona+"
                                    // "+i.zona.charAt(1));
                                    if (Zona == i.zona.charAt(1)) {
                                        PreparedStatement aler = conn.prepareStatement(
                                                "INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) "
                                                        + "VALUES('" + a + "','" + 0 + "','ALERTA INFERIOR do sensor "
                                                        + i.sensor + " da zona " + i.zona + ".','" + id_cultura + "','"
                                                        + id_medicao + "','" + decimal + "')");
                                        aler.executeUpdate();
                                        foundAlert = true;
                                    }
                                } else if (Float.valueOf(i.medicao) <= Limite_Inf_Critico_Temp) {
                                    // System.out.println( i.sensor.startsWith("T") +" "+ Zona+"
                                    // "+i.zona.charAt(1));
                                    if (Zona == i.zona.charAt(1)) {
 
                                        PreparedStatement aler = conn.prepareStatement(
                                                "INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) "
                                                        + "VALUES('" + a + "','" + 1
                                                        + "','ALERTA CRÍTICO INFERIOR do sensor " + i.sensor
                                                        + " da zona " + i.zona + ".','" + id_cultura + "','"
                                                        + id_medicao + "','" + decimal + "')");
                                        aler.executeUpdate();
                                        foundAlert = true;
                                    }
                                }
                            } else if (i.sensor.startsWith("H")) {
                                if (Limite_Sup_Hum <= Float.valueOf(i.medicao)
                                        && Float.valueOf(i.medicao) <= Limite_Sup_Critico_Hum) {
                                    // System.out.println( i.sensor.startsWith("H") +" "+ Zona+"
                                    // "+i.zona.charAt(1));
                                    if (Zona == i.zona.charAt(1)) {
                                        PreparedStatement aler = conn.prepareStatement(
                                                "INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) "
                                                        + "VALUES('" + a + "','" + 0 + "','ALERTA SUPERIOR do sensor "
                                                        + i.sensor + " da zona " + i.zona + ".','" + id_cultura + "','"
                                                        + id_medicao + "','" + decimal + "')");
                                        aler.executeUpdate();
                                        foundAlert = true;
                                    }
                                } else if (Float.valueOf(i.medicao) >= Limite_Sup_Critico_Hum) {
                                    // System.out.println( i.sensor.startsWith("H") +" "+ Zona+"
                                    // "+i.zona.charAt(1));
                                    if (Zona == i.zona.charAt(1)) {
                                        PreparedStatement aler = conn.prepareStatement(
                                                "INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura )  "
                                                        + "VALUES('" + a + "','" + 1
                                                        + "','ALERTA CRÍTICO SUPERIOR do sensor " + i.sensor
                                                        + " da zona " + i.zona + ".','" + id_cultura + "','"
                                                        + id_medicao + "','" + decimal + "')");
                                        aler.executeUpdate();
                                        foundAlert = true;
                                    }
                                } else if (Limite_Inf_Hum >= Float.valueOf(i.medicao)
                                        && Float.valueOf(i.medicao) >= Limite_Inf_Critico_Hum) {
                                    // System.out.println( i.sensor.startsWith("H") +" "+ Zona+"
                                    // "+i.zona.charAt(1));
                                    if (Zona == i.zona.charAt(1)) {
                                        PreparedStatement aler = conn.prepareStatement(
                                                "INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) "
                                                        + "VALUES('" + a + "','" + 0 + "','ALERTA INFERIOR do sensor "
                                                        + i.sensor + " da zona " + i.zona + ".','" + id_cultura + "','"
                                                        + id_medicao + "','" + decimal + "')");
                                        aler.executeUpdate();
                                        foundAlert = true;
                                    }
 
                                } else if (Float.valueOf(i.medicao) <= Limite_Inf_Critico_Hum) {
                                    // System.out.println( i.sensor.startsWith("H") +" "+ Zona+"
                                    // "+i.zona.charAt(1));
                                    if (Zona == i.zona.charAt(1)) {
                                        PreparedStatement aler = conn.prepareStatement(
                                                "INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) "
                                                        + "VALUES('" + a + "','" + 1
                                                        + "','ALERTA CRÍTICO INFERIOR do sensor " + i.sensor
                                                        + " da zona " + i.zona + ".','" + id_cultura + "','"
                                                        + id_medicao + "','" + decimal + "')");
                                        aler.executeUpdate();
                                        foundAlert = true;
                                    }
                                }
                            } else if (i.sensor.startsWith("L")) {
                                // System.out.println( i.sensor.startsWith("L") +" "+ Zona+"
                                // "+i.zona.charAt(1));
                                if (Limite_Sup_Luz <= Float.valueOf(i.medicao)
                                        && Float.valueOf(i.medicao) <= Limite_Sup_Critico_Luz) {
                                    if (Zona == i.zona.charAt(1)) {
                                        PreparedStatement aler = conn.prepareStatement(
                                                "INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) "
                                                        + "VALUES('" + a + "','" + 0 + "','ALERTA SUPERIOR do sensor "
                                                        + i.sensor + " da zona " + i.zona + ".','" + id_cultura + "','"
                                                        + id_medicao + "','" + decimal + "')'");
                                        aler.executeUpdate();
                                        foundAlert = true;
                                    }
                                } else if (Float.valueOf(i.medicao) >= Limite_Sup_Critico_Luz) {
                                    // System.out.println( i.sensor.startsWith("L") +" "+ Zona+"
                                    // "+i.zona.charAt(1));
                                    if (Zona == i.zona.charAt(1)) {
                                        PreparedStatement aler = conn.prepareStatement(
                                                "INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura )  "
                                                        + "VALUES('" + a + "','" + 1
                                                        + "','ALERTA CRÍTICO SUPERIOR do sensor " + i.sensor
                                                        + " da zona " + i.zona + ".','" + id_cultura + "','"
                                                        + id_medicao + "','" + decimal + "')");
                                        aler.executeUpdate();
                                        foundAlert = true;
                                    }
                                } else if (Limite_Inf_Luz >= Float.valueOf(i.medicao)
                                        && Float.valueOf(i.medicao) >= Limite_Inf_Critico_Luz) {
                                    // System.out.println( i.sensor.startsWith("L") +" "+ Zona+"
                                    // "+i.zona.charAt(1));
                                    if (Zona == i.zona.charAt(1)) {
                                        PreparedStatement aler = conn.prepareStatement(
                                                "INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) "
                                                        + "VALUES('" + a + "','" + 0 + "','ALERTA INFERIOR do sensor "
                                                        + i.sensor + " da zona " + i.zona + ".','" + id_cultura + "','"
                                                        + id_medicao + "','" + decimal + "')");
                                        aler.executeUpdate();
                                        foundAlert = true;
                                    }
                                } else if (Float.valueOf(i.medicao) <= Limite_Inf_Critico_Luz) {
                                    // System.out.println( i.sensor.startsWith("L") +" "+ Zona+"
                                    // "+i.zona.charAt(1));
                                    if (Zona == i.zona.charAt(1)) {
                                        PreparedStatement aler = conn.prepareStatement(
                                                "INSERT INTO alerta(Hora, Tipo_Alerta, Mensagem, ID_Cultura, ID_Medicao, Leitura ) VALUES('"
                                                        + a + "','" + 1 + "','ALERTA CRÍTICO INFERIOR do sensor "
                                                        + i.sensor + " da zona " + i.zona + ".','" + id_cultura + "','"
                                                        + id_medicao + "','" + decimal + "')");
                                        aler.executeUpdate();
                                        foundAlert = true;
                                    }
                                }
                            }
                            if (foundAlert) {
                                boolean alreadyExistsAlert = false;
                                for (Pair<Integer, Timestamp> alert : lastAlerts) {
                                        if (alert.first == id_cultura) {
                                            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                                            alert.second = timestamp;
                                            alreadyExistsAlert = true;
                                        }
                                    }
                                    if(!alreadyExistsAlert) {
                                        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                                        Pair<Integer, Timestamp> newAlert = new Pair<Integer, Timestamp>(id_cultura, timestamp);
                                        lastAlerts.add(newAlert);
                                    }
                                }
 
                        }
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
 
    }
    //fazer para as duas culturas
    private boolean hasTimeoutPassed(ArrayList<Pair<Integer, Timestamp>> lastAlerts,
            ArrayList<Pair<Integer, Integer>> timeouts, int id_cultura) {
    	
    	for (Pair<Integer, Timestamp> cult : lastAlerts) {
            System.out.println(cult.first+"-------------------has"+ id_cultura );
    		if (id_cultura == cult.first) {
            	System.out.println(System.currentTimeMillis()+"-------------------primeiro if");
            	System.out.println(cult.second.getTime()+ "-------------------´++");
                for (Pair<Integer, Integer> tm : timeouts) {
                    if (id_cultura == tm.first) {
                    	System.out.println(tm.second);
                    	if (System.currentTimeMillis() - cult.second.getTime() > tm.second * 1000) {
                        	System.out.println("entrou no if");
                        	return true;
                        }
                    }
                }
            }
        }
        return false;
    }
    
    private boolean hasAlertFromCult(int id, ArrayList<Pair<Integer, Timestamp>> lastAlerts) {
        for(Pair<Integer, Timestamp> cult : lastAlerts) {
            if (cult.first == id) {
                return true;
            }
        }
        return false;
    }
 
    public static void main(String[] args) throws SQLException {
        AppSettings settings = null;

        File f = new File("settings.json");
        Gson gson = new Gson();
        FileReader fr;
        try {
            System.out.println("Reading settings from: " + f.getAbsolutePath());
            fr = new FileReader(f);
            settings = gson.fromJson(fr, AppSettings.class);
            fr.close();
        } catch (FileNotFoundException e) {
            System.err.println("Settings file not found! Creating one for you. The application will then close");
            AppSettings.createModel();
            return;
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        Mysql_demo demo = new Mysql_demo();
        MongoSettings mongo = settings.mongo;
        demo.mongodbConnection(mongo.ip, mongo.port, mongo.database, mongo.collection);
        SqlSettings sql = settings.sql;
        demo.mysqlConnection(sql.ip, sql.port, sql.db, sql.credentials.username, sql.credentials.password);
 
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