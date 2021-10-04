package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districtTrees", DistrictTrees.class,
                    "A map/reduce program that displays the list of distinct trees.");
            programDriver.addClass("species", Species.class,
                    "A map/reduce program that displays the list of different species trees.");
            programDriver.addClass("numberTrees", NumberTrees.class,
                    "A map/reduce program that calculates the number of trees of each kind.");
            programDriver.addClass("maxHeight", MaxHeight.class,
                    "A map/reduce program that calculates the height of the tallest tree of each kind.");
            programDriver.addClass("sortHeight", SortHeight.class,
                    "A map/reduce program that sort the tree heights from smallest to largest.");
            programDriver.addClass("oldestTree", OldestTree.class,
                    "A map/reduce program that displays the district where the oldest tree is.");
            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
