import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class ImportDatabase {
	
	private static final String CONNECTOR = "jdbc:mysql://localhost:3306/";
	private static final String DATABASE_NAME = "Luteg";
	
	private static final String USERNAME = "username";
	private static final String PASSWORD = "password";
	
	private static final String COMMA_DELIMITER = ",";
	
	
	public void loadCSV(Path child) throws IOException {
		
		// check the first line for the column length
		try {
			BufferedReader fileReader = new BufferedReader(new FileReader(child.toString()));
			String line = fileReader.readLine();
			fileReader.close();
			String[] headerRow = line.split(COMMA_DELIMITER);	
			if (headerRow.length != 6) {
				System.err.println("CSV file should have six column");
				return;
			}			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		
		// load the file into database 
		try {
			Connection connection = DriverManager.getConnection(CONNECTOR + DATABASE_NAME, USERNAME, PASSWORD);
			String loadQuery = "LOAD DATA LOCAL INFILE '" 
									+ child.toString() 
									+ "' REPLACE INTO TABLE Luteg"
									+ " FIELDS TERMINATED BY ','"
									+ " LINES TERMINATED BY '\r\n' "
									+ " IGNORE 1 LINES "
									+ " (CourseName, StudentId, Vize1, Vize2, Vize3, Final)";
            
			PreparedStatement stmt = connection.prepareStatement(loadQuery);
            stmt.execute();
            connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
