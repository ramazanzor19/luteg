import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;

public class Main {

	
	public static void main(String[] args) throws IOException, SQLException {
		
		if (args.length == 0 || args.length > 1) {
			System.err.println("usage: java Main DIR_PATH");
			System.exit(-1);
		}
		
		String path = args[0];
		Path directory = Paths.get(path);
		
		// register directory and start the loop
		WatchDirectory service = new WatchDirectory(directory);
		service.process();
	}
}
