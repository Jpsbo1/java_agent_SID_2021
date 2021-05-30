package mongoToMysql.settings;

public class SqlSettings {
    public String ip = "localhost";
    public int port = 3306;
    public String db = "lab";
    public Auth credentials = new Auth("root", "");
}
