import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.lang.System;

public class FileMgmt {
  public static void main(String[] args) throws Exception{
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    switch(args[0]) {
      case "mkdir":
        mkdir(fs, args[1]);
        break;

      case "ls":
        ls(fs, args[1]);
        break;

      case "cat":
        cat(fs, args[1]);
        break;

      case "cp":
        copy(fs, args[1], args[2]);
        break;

      case "mv":
        copy(fs, args[1], args[2]);
        rm(fs, args[1], true);
        break;

      case "rm":
        rm(fs, args[1], false);
        break;

      case "get":
        get(fs, args[1], args[2]);
        break;

      case "put":
        put(fs, args[1], args[2]);
        break;

      default:
        System.out.print("Illegal command: ");
        System.out.println(args[0]);
    }

  }

  public static void mkdir(FileSystem fs, String pathname) throws Exception {
    boolean res = fs.mkdirs(new Path(pathname));

    if (res) {
      System.out.print(pathname);
      System.out.println(" directory created.");
    } else {
      System.out.print("Error while creating directory ");
      System.out.println(pathname);
    }
  }

  public static void ls(FileSystem fs, String pathname) throws Exception {
    FileStatus[] stats = fs.listStatus(new Path(pathname));
    Path[] fnames = FileUtil.stat2Paths(stats);

    for (Path p: fnames) {
      System.out.println(p);
    }
  }

  public static void cat(FileSystem fs, String pathname) throws Exception {
    InputStream in = null;
    try {
      in = fs.open(new Path(pathname));
      IOUtils.copyBytes(in, System.out, 4096, false);
    } finally {
      IOUtils.closeStream(in);
    }
  }

  public static void copy(FileSystem fs, String src, String dst) throws Exception {
    FSDataInputStream in = null;
    FSDataOutputStream out = null;
    try {
      in = fs.open(new Path(src));
      out = fs.create(new Path(dst));
      IOUtils.copyBytes(in, out, 4096, false);
    } finally {
      IOUtils.closeStream(in);
      IOUtils.closeStream(out);
    }
  }

  public static void rm(FileSystem fs, String pathname, boolean silent) throws Exception {
    boolean ret = fs.delete(new Path(pathname), true);
    if(silent) return;

    if (ret) {
      System.out.print("Successfully deleted ");
    } else {
      System.out.print("Error! Cannot delete ");
    }
    System.out.println(pathname);
  }

  public static void put(FileSystem fs, String src, String dst) throws Exception {
    InputStream in = null;
    FSDataOutputStream out = null;
    try{
      in = new BufferedInputStream(new FileInputStream(src));
      out = fs.create(new Path(dst));
      IOUtils.copyBytes(in, out, 4096, false);
    } finally {
      IOUtils.closeStream(in);
      IOUtils.closeStream(out);
    }
  }

  public static void get(FileSystem fs, String src, String dst) throws Exception {
    FSDataInputStream in = null;
    OutputStream out = null;
    try {
      in = fs.open(new Path(src));
      out = new BufferedOutputStream(new FileOutputStream(dst));
      IOUtils.copyBytes(in, out, 4096, false);
    } finally {
      IOUtils.closeStream(in);
      IOUtils.closeStream(out);
    }
  }
}