package com.sproutsocial.nsq;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestBase {

    //public static long seed = System.currentTimeMillis();
    public static long seed = 1447451719500L;
    public static Random random = new Random(seed);

    public static List<String> messages(int count, int maxLen) {
        List<String> res = new ArrayList<String>(count);
        for (int i = 0; i < count; i++) {
            int len = random.nextInt(maxLen);
            StringBuilder s = new StringBuilder();
            s.append(String.format("msg %04d len:%04d ", i , len));
            for (int j = 0; j < len; j++) {
                s.append((char) (33 + random.nextInt(92)));
            }
            res.add(s.toString());
        }
        return res;
    }

    public static void exec(String command) throws IOException, InterruptedException {
        Process proc = Runtime.getRuntime().exec(command);
        proc.waitFor();
        readInput(proc.getInputStream());
        readInput(proc.getErrorStream());
        System.out.println(command + " exit:" + proc.exitValue());
    }

    private static void readInput(InputStream inputStream) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
        String line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
    }

    public static void debugFail(List<String> testMsgs, List<String> targetMsgs) {
        System.out.println("test size:" + testMsgs.size() + " target size:" + targetMsgs.size());
        for (int i = 0; i < testMsgs.size(); i++) {
            String msg = testMsgs.get(i);
            String target = targetMsgs.get(i);
            System.out.println("test:" + msg.substring(0, Math.min(40, msg.length())));
            System.out.println(" tgt:" + target.substring(0, Math.min(40, target.length())));
            if (!msg.equals(target)) {
                System.out.println(msg);
                System.out.println(target);
                System.out.println("FAIL");
                break;
            }
            System.out.println();
        }
    }

}
