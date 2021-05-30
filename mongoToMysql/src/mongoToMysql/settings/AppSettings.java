package mongoToMysql.settings;

import com.google.gson.Gson;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class AppSettings {

    public MongoSettings mongo = new MongoSettings();
    public SqlSettings sql = new SqlSettings();

    public static void createModel(){
        AppSettings as = new AppSettings();
        FileWriter fw;
        try {
            fw = new FileWriter(new File("settings.json"));
            Gson gson = new Gson();
            fw.write(gson.toJson(as));
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
