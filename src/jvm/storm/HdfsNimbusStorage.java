package storm;

import backtype.storm.nimbus.INimbusStorage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author slukjanov
 */
public class HdfsNimbusStorage implements INimbusStorage {
    public static final String NIMBUS_STORAGE_HDFS_DIR = "nimbus.storage.hdfs.dir";
    public static final String NIMBUS_STORAGE_HDFS_PATH = "nimbus.storage.hdfs.path";
    public static final String NIMBUS_STORAGE_HDFS_USER = "nimbus.storage.hdfs.user";

    private FileSystem fs;
    private String dir;

    @Override
    public void init(Map conf) {
        try {
            String pathString = (String) conf.get(NIMBUS_STORAGE_HDFS_PATH);

            String user = (String) conf.get(NIMBUS_STORAGE_HDFS_USER);
            if (user != null && user.trim().length() > 0) {
                System.setProperty("HADOOP_USER_NAME", user.trim());
            }
            fs = new Path(pathString).getFileSystem(new Configuration());

            String dirString = (String) conf.get(NIMBUS_STORAGE_HDFS_DIR);
            if (dirString != null && dirString.trim().length() > 0) {
                dir = dirString.trim();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream open(String path) {
        try {
            return fs.open(new Path(prependDir(path)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public OutputStream create(String path) {
        try {
            return fs.create(new Path(prependDir(path)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> list(String path) {
        try {
            FileStatus[] fileStatuses = fs.listStatus(new Path(prependDir(path)));
            if (fileStatuses == null) {
                return Collections.emptyList();
            }

            List<String> result = new ArrayList<String>();
            for (FileStatus fileStatus : fileStatuses) {
                result.add(fileStatus.getPath().getName());
            }

            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(String path) {
        try {
            fs.delete(new Path(prependDir(path)), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void mkdirs(String path) {
        try {
            fs.mkdirs(new Path(prependDir(path)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isSupportDistributed() {
        return true;
    }

    private String prependDir(String path) {
        if (!dir.endsWith("/") && !path.startsWith("/")) {
            path = "/" + path;
        }
        return dir + path;
    }
}
