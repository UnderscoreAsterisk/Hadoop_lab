import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.FileInputStream;

public class Upload {
  public static void main(String[] args) throws Exception {
    String src = args[0];
    String dst = args[1];

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    InputStream in = null;
    FSDataOutputStream out = null;
    try{
      in = new BufferedInputStream(new FileInputStream(src));
      out = fs.create(new Path(dst));
      IOUtils.copyBytes(in, out, conf);
    } finally {
      IOUtils.closeStream(in);
      IOUtils.closeStream(out);
    }
  }
}