import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.sql.SQLException;

public class WatchDirectory {

	private final Path directory;
	private final WatchService watcher;
    private WatchKey key;
    private final ImportDatabase importDatabase;
        
    @SuppressWarnings("unchecked")
    static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>)event;
    }
    
    /**
     * Creates a WatchService and registers the given directory
     * @throws SQLException 
     */
    WatchDirectory(Path directory) throws IOException, SQLException {
    	
    	this.directory = directory;
    	this.watcher = FileSystems.getDefault().newWatchService();     
    	this.key = this.directory.register(watcher, ENTRY_CREATE);
    	this.importDatabase = new ImportDatabase();
	}
    
    /**
     * Process all events
     * @throws IOException  
     */
    public void process() throws IOException {
    	for (;;) {
    		
    		// wait for key to be signalled
            try {
				key = watcher.take();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
            for (WatchEvent<?> event: key.pollEvents()) {
                Kind<?> kind = event.kind();
                
                if (kind == OVERFLOW) {
                    continue;
                }
                
                
                WatchEvent<Path> ev = cast(event);
                Path name = ev.context();
                Path child = directory.resolve(name);
                
                // look if it is a csv file
                if (event.kind().name() == "ENTRY_CREATE" && child .toString().endsWith(".csv")) {
                	// load the file into database
                	importDatabase.loadCSV(child);
             	
                }
                
                // delete the file
                File file = child.toFile();
                delete(file);
                
                // reset the key
                boolean valid = key.reset();
                if (!valid) {
                	break;
                }
            }
    	}
    }
    
    /**
     * Delete the file or directory added to watched directory
     * @throws IOException  
     */
	void delete(File f) throws IOException {
		if (f.isDirectory()) {
			for (File c : f.listFiles())
				delete(c);
		}
		if (!f.delete())
			throw new FileNotFoundException("Failed to delete file: " + f);
	}
	
}
