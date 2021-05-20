package mongoToMysql;

import java.util.Date;

public class SensorData {
    public String zona;
    public String sensor;
    public String medicao;
    public Date data;

    public SensorData(String zona, String sensor, String medicao, Date data) {
        this.zona = zona;
        this.sensor = sensor;
        this.medicao = medicao;
        this.data = data;
    }

    @Override
    public String toString() {
        return "SensorData{" +
                "zona='" + zona + '\'' +
                ", sensor='" + sensor + '\'' +
                ", medicao='" + medicao + '\'' +
                ", data=" + data +
                '}';
    }
}