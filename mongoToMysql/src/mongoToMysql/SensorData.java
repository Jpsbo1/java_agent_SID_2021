package mongoToMysql;

public class SensorData {
    public String zona;
    public String sensor;
    public String medicao;

    public SensorData(String zona, String sensor, String medicao) {
        this.zona = zona;
        this.sensor = sensor;
        this.medicao = medicao;
    }
}
