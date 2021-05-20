package mongoToMysql;

import java.util.Date;

public class MongoCloudDocument {
    public class Id{
        public String oid;
    }
    public Id _id;
    public String Zona;
    public String Sensor;
    public Date Data;
    public String Medicao;
}