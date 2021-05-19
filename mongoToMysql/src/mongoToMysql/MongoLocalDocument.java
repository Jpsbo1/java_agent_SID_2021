package mongoToMysql;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MongoLocalDocument {

    public Date data;
    public List<SensorData> sensors;

    public MongoLocalDocument() {
        sensors = new ArrayList<>();
    }

    public String getJson(){
        Gson gson = new Gson();
        return gson.toJson(this);
    }

}

