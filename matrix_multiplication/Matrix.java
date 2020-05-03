import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.stream.IntStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Matrix {
  public static class NonSplittableKVInputFormat extends KeyValueTextInputFormat{

    @Override
    protected boolean isSplitable(JobContext ctx, Path file) {
      return false;
    }
  }

  static class MatrixAddress extends IntWritable {
    private IntWritable matrix_no;
    private IntWritable row;
    private IntWritable col;
    private IntWritable targ_dim;

    public MatrixAddress() {
      set(new IntWritable(), new IntWritable(), new IntWritable(), new IntWritable());
    }

    public MatrixAddress(int m, int r, int c, int t_d) {
      set(new IntWritable(m), new IntWritable(r), new IntWritable(c), new IntWritable(t_d));
    }

    public void set(IntWritable m, IntWritable r, IntWritable c, IntWritable t_d) {
      this.matrix_no = m;
      this.row = r;
      this.col = c;
      this.targ_dim = t_d;
    }

    public IntWritable getMat() {return matrix_no;}
    public IntWritable getRow() {return row;}
    public IntWritable getCol() {return col;}
    public IntWritable getTargetDim() {return targ_dim;}

    @Override
    public void write(DataOutput out) throws IOException {
      matrix_no.write(out);
      row.write(out);
      col.write(out);
      targ_dim.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      matrix_no.readFields(in);
      row.readFields(in);
      col.readFields(in);
      targ_dim.readFields(in);
    }

    @Override
    public int hashCode() {
      return 163*(163*(163*matrix_no.hashCode()+row.hashCode())+col.hashCode())+targ_dim.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof MatrixAddress) {
        MatrixAddress addr = (MatrixAddress) o;
        return
          matrix_no.equals(addr.matrix_no) &&
          row.equals(addr.row) &&
          col.equals(addr.col);
      }
      return false;
    }
  }

  static class MatrixMapper
    extends Mapper<Text, Text, MatrixAddress, IntWritable> {

      Path fname;
      private int matrix_no;

      @Override
      protected void setup(Context context) throws IOException, InterruptedException {
        InputSplit split = context.getInputSplit();
        fname = ((FileSplit) split).getPath();
      }

      @Override
      protected void map(Text key, Text value, Context context) throws
        IOException, InterruptedException {
          Configuration conf = context.getConfiguration();
          FileSystem fs = FileSystem.get(conf);

          Path file1 = new Path(conf.get("matrixA"));
          Path file2 = new Path(conf.get("matrixB"));

          if(fname.compareTo(file1.makeQualified(fs)) == 0) {
            matrix_no = 0;
          } else if (fname.compareTo(file2.makeQualified(fs)) == 0) {
            matrix_no = 1;
          } else {
            System.out.println(String.format("No match found for %s", fname.toString()));
          }

          String rowStr = value.toString();
          StringTokenizer itr = new StringTokenizer(rowStr);
          int row = Integer.parseInt(key.toString());
          int col = 1;
          while(itr.hasMoreTokens()) {
              int val = Integer.parseInt(itr.nextToken(",").trim());
              String dim = matrix_no==0?"k":"i";
              int duplicates = Integer.parseInt(conf.get(dim));

              int targ_row = row;
              int targ_col = col;

              for(int i = 1; i <= duplicates; i++) {

                if(matrix_no == 0) targ_col = i;
                else targ_row = i;
                int targ_num_cols = Integer.parseInt(conf.get("k"));
                int targ_dim = (targ_row-1)*targ_num_cols+(targ_col-1);

                MatrixAddress maddr = new MatrixAddress(matrix_no, row, col, targ_dim);
                context.write(maddr, new IntWritable(val));
              }

              col++;
            }
        }
    }

  public static class SortComparator extends WritableComparator {
    protected SortComparator() {
      super(MatrixAddress.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      MatrixAddress m1 = (MatrixAddress) w1;
      MatrixAddress m2 = (MatrixAddress) w2;
      int cmp1 = m1.getTargetDim().compareTo(m2.getTargetDim());
      int cmp2 = m1.getMat().compareTo(m2.getMat());
      int cmp3 = m1.getRow().compareTo(m2.getRow());
      if (cmp1 == 0)
        if (cmp2 == 0)
          if (cmp3 == 0)
            return m1.getCol().compareTo(m2.getCol());
          else return cmp3;
         else return cmp2;
      else return cmp1;
    }
  }

  public static class TargetComparator extends WritableComparator {
    protected TargetComparator() {
      super(MatrixAddress.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      MatrixAddress m1 = (MatrixAddress) w1;
      MatrixAddress m2 = (MatrixAddress) w2;
      return m1.getTargetDim().get()-m2.getTargetDim().get();
    }
  }

  public static class MatrixReducer extends Reducer<MatrixAddress, IntWritable, Text, NullWritable> {

    String outString = "";

    @Override
    protected void reduce(MatrixAddress key, Iterable<IntWritable> values, Context context) throws
      IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int numElems = Integer.parseInt(conf.get("j"));
        int numCols = Integer.parseInt(conf.get("k"));
        int[] accum = new int[numElems];
        Arrays.fill(accum, 1);

        int matrix_no = key.getMat().get();
        int row = key.getRow().get();
        int col = key.getCol().get();
        int targ_dim = key.getTargetDim().get();

        int i = 0;
        for(IntWritable itr: values) {
          int val = itr.get();
          accum[i%numElems] *= val;
          i++;
        }
        int elem = IntStream.of(accum).sum();
        if (targ_dim%numCols == 0) outString = String.valueOf(elem);
        else outString += String.format(", %d", elem);

        if(targ_dim%numCols == (numCols-1)) {
          context.write(new Text(outString), NullWritable.get());
          outString = "";
        }
      }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    int rows1 = Integer.parseInt(args[0]);
    conf.set("i", String.valueOf(rows1));

    int cols1 = Integer.parseInt(args[1]);
    conf.set("j", String.valueOf(cols1));
    String file1 = args[2];

    int rows2 = Integer.parseInt(args[3]);
    int cols2 = Integer.parseInt(args[4]);
    conf.set("k", String.valueOf(cols2));

    String file2 = args[5];

    if (cols1 != rows2) {
      System.out.println("Cannot multiply matrices, incompatible dimensions");
      System.exit(1);
    }

    Path p = new Path(file1);
    conf.set("matrixA", file1);
    conf.set("matrixB", file2);
    Job job = Job.getInstance(conf, "matrix multiplication");
    job.setJarByClass(Matrix.class);

    job.setInputFormatClass(NonSplittableKVInputFormat.class);
    job.setMapperClass(MatrixMapper.class);

    job.setMapOutputKeyClass(MatrixAddress.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setSortComparatorClass(SortComparator.class);
    job.setGroupingComparatorClass(TargetComparator.class);

    job.setNumReduceTasks(1);
    job.setReducerClass(MatrixReducer.class);

    FileInputFormat.addInputPath(job, new Path(file1));
    FileInputFormat.addInputPath(job, new Path(file2));
    FileOutputFormat.setOutputPath(job, new Path(args[6]));

    job.waitForCompletion(false);
  }
}