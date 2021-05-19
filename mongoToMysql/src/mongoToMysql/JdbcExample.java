package mongoToMysql;
import java.sql.*;
public class JdbcExample {
	@SuppressWarnings("deprecation")
	public static void main(String args[]) {
		Connection con = null;
		try {
			Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/lab","root", "");
			if (!con.isClosed())
				System.out.println("Successfully connected to MySQL server...");
		} catch(Exception e) {
			System.err.println("Exception: " + e.getMessage());
		} finally {
			try {
				if (con != null)
					con.close();
			}catch(SQLException e) {}
		}
	}
}
