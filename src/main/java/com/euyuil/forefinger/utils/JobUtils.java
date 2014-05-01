package com.euyuil.forefinger.utils;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

/**
 * @author Liu Yue
 * @version 0.0.2014.05.01
 */
public class JobUtils {

    private static final long WAIT_SLEEP = 1000;

    public static boolean waitForCompletion(Collection<Job> jobs) {
        boolean result = true;
        HashSet<Job> incompleteJobs = new HashSet<Job>(jobs);
        while (!incompleteJobs.isEmpty()) {
            System.out.println(String.format("%d jobs running...", incompleteJobs.size()));
            try {
                Thread.sleep(WAIT_SLEEP);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Iterator<Job> iterator = incompleteJobs.iterator();
            while (iterator.hasNext()) {
                Job job = iterator.next();
                try {
                    if (job.isComplete()) {
                        if (!job.isSuccessful()) {
                            result = false;
                        } else {
                            iterator.remove();
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (!result) {
                for (Job job : incompleteJobs) {
                    try {
                        job.killJob();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                break;
            }
        }
        return result;
    }
}
