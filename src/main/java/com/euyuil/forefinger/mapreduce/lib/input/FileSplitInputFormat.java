package com.euyuil.forefinger.mapreduce.lib.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * File split contains the path of the file, the start position, and the length of the split.
 * The input format is for Hadoop to read such file splits.
 * If you know the position of your desired data (from index), you can use this input format.
 *
 * @author Liu Yue
 * @version 0.0.2014.04.11
 */
public class FileSplitInputFormat extends TextInputFormat {

    /**
     * Input split parameter.
     * Format: fileName,startPos,length|fileName2,startPos2,length2|...
     */
    private static String PARAM_INPUT_SPLIT = "forefinger.input.split";

    private static String PARAM_NUM_INPUT_SPLITS = "forefinger.input.split.count";

    private static String PARAM_NUM_INPUT_FILES = "forefinger.input.file.count";

    private static double SPLIT_SLOP = 1.1;

    private static final PathFilter hiddenFileFilter = new PathFilter() {
        @Override
        public boolean accept(Path path) {
            String name = path.getName();
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };

    /**
     * Constructs the input split parameter for the job.
     *
     * @param job the job spec object.
     * @param fileSplit the file split.
     * @throws IOException
     */
    public static void addFileSplit(Job job, FileSplitDescriptor fileSplit) throws IOException {
        Configuration conf = job.getConfiguration();
        Path path = fileSplit.getPath().getFileSystem(conf).makeQualified(fileSplit.getPath());
        String splitStr = String.format("%s,%d,%d",
                StringUtils.escapeString(path.toString()), fileSplit.getStart(), fileSplit.getLength());
        String splits = conf.get(PARAM_INPUT_SPLIT);
        conf.set(PARAM_INPUT_SPLIT, splits == null ? splitStr : splits + "|" + splitStr);
    }

    public static FileSplitDescriptor[] getFileSplits(JobContext context) {
        String strParamInputSplit = context.getConfiguration().get(PARAM_INPUT_SPLIT);
        String[] strInputSplits = strParamInputSplit.split("|");
        FileSplitDescriptor[] fileSplits = new FileSplitDescriptor[strInputSplits.length];
        for (int i = 0; i < strInputSplits.length; i++) {
            String[] splits = strInputSplits[i].split(",");
            fileSplits[i] = new FileSplitDescriptor(
                    new Path(splits[0]), Integer.valueOf(splits[1]), Integer.valueOf(splits[2]));
        }
        return fileSplits;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return super.createRecordReader(split, context);
    }

    protected List<FileStatusDescriptor> listFileStatus(JobContext job) throws IOException {

        List<FileStatusDescriptor> result = new ArrayList<FileStatusDescriptor>();
        FileSplitDescriptor[] fileSplits = getFileSplits(job);

        if (fileSplits.length == 0)
            throw new IOException("No input splits specified in job");

        Path[] paths = new Path[fileSplits.length];
        for (int i = 0; i < fileSplits.length; i++) {
            FileSplitDescriptor fileSplit = fileSplits[i];
            paths[i] = fileSplit.getPath();
        }

        // Get tokens for all the required FileSystems.
        TokenCache.obtainTokensForNamenodes(job.getCredentials(), paths, job.getConfiguration());

        List<IOException> errors = new ArrayList<IOException>();

        for (int i = 0; i < fileSplits.length; ++i) {
            Path path = fileSplits[i].getPath();
            FileSystem fs = path.getFileSystem(job.getConfiguration());
            FileStatus[] matches = fs.globStatus(path, hiddenFileFilter); // TODO Multi filter
            if (matches == null)
                errors.add(new IOException(String.format("Input path does not exist %s", path)));
            else if (matches.length == 0)
                errors.add(new IOException(String.format("Input pattern %s matches 0 files", path)));
            else for (FileStatus globStat : matches) {
                if (globStat.isDir())
                    errors.add(new IOException(String.format("Input path %s is a directory", path)));
                else
                    result.add(new FileStatusDescriptor(globStat, fileSplits[i].getStart(), fileSplits[i].getLength()));
            }
        }

        if (!errors.isEmpty())
            throw new InvalidInputException(errors);

        return result;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {

        long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
        long maxSize = getMaxSplitSize(job);

        List<InputSplit> result = new ArrayList<InputSplit>();
        List<FileStatusDescriptor> fileSplits = listFileStatus(job);

        for (FileStatusDescriptor fileSplit : fileSplits) {

            FileStatus fileStatus = fileSplit.getFileStatus();
            Path path = fileStatus.getPath();
            FileSystem fs = path.getFileSystem(job.getConfiguration());

            // TODO Maybe we don't have to get the block locations of the whole file?
            long length = fileStatus.getLen();
            BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, length);

            if ((length != 0) && isSplitable(job, path)) {

                long blockSize = fileStatus.getBlockSize();
                long splitSize = computeSplitSize(blockSize, minSize, maxSize);

                long actualStart = (fileSplit.getStart() / blockSize) * blockSize;
                long actualLength = fileSplit.getStart() + fileSplit.getLength() - actualStart;

                long bytesRemaining = actualLength;

                // Add splits
                while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
                    long splitStart = actualLength - bytesRemaining + actualStart;
                    int blockIndex = getBlockIndex(blockLocations, splitStart);
                    result.add(new FileSplit(path, splitStart, splitSize,
                            blockLocations[blockIndex].getHosts()));
                    bytesRemaining -= splitSize;
                }

                // Add last small piece
                if (bytesRemaining != 0) {
                    long splitStart = actualLength - bytesRemaining + actualStart;
                    result.add(new FileSplit(path, splitStart, bytesRemaining,
                            blockLocations[blockLocations.length - 1].getHosts()));
                }
            } else if (length != 0) {
                result.add(new FileSplit(path, 0, length, blockLocations[0].getHosts()));
            } else {
                result.add(new FileSplit(path, 0, length, new String[0]));
            }
        }

        job.getConfiguration().setLong(PARAM_NUM_INPUT_SPLITS, result.size());
        job.getConfiguration().setLong(PARAM_NUM_INPUT_FILES, fileSplits.size());

        return result;
    }

    public static class FileSplitDescriptor {

        private Path path;

        private long start;

        private long length;

        public FileSplitDescriptor(Path path, long start, long length) {
            this.path = path;
            this.start = start;
            this.length = length;
        }

        public Path getPath() {
            return path;
        }

        public void setPath(Path path) {
            this.path = path;
        }

        public long getStart() {
            return start;
        }

        public void setStart(long start) {
            this.start = start;
        }

        public long getLength() {
            return length;
        }

        public void setLength(long length) {
            this.length = length;
        }
    }

    public static class FileStatusDescriptor {

        private FileStatus fileStatus;

        private long start;

        private long length;

        public FileStatusDescriptor(FileStatus fileStatus, long start, long length) {
            this.fileStatus = fileStatus;
            this.start = start;
            this.length = length;
        }

        public FileStatus getFileStatus() {
            return fileStatus;
        }

        public void setFileStatus(FileStatus fileStatus) {
            this.fileStatus = fileStatus;
        }

        public long getStart() {
            return start;
        }

        public void setStart(long start) {
            this.start = start;
        }

        public long getLength() {
            return length;
        }

        public void setLength(long length) {
            this.length = length;
        }
    }
}
