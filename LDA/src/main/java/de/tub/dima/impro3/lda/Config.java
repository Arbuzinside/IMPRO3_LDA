package de.tub.dima.impro3.lda;

/**
 * Created by arbuzinside on 7.2.2016.
 */
public class Config {



    private static final String INPUT_PATH = "/home/dmatar/impro/";
    private static final String OUTPUT_PATH = "/home/dmatar/results/";

// private static final String INPUT_PATH = "/home/arbuzinside/workspace/assignments/Assignmnet2/classification/src/test/resources/assignment2/";
    //private static final String OUTPUT_PATH = "/home/arbuzinside/workspace/assignments/Assignmnet2/classification/output/";










    public static String pathToTrainingSet() {
        return INPUT_PATH + "train.tab";

    }

    public static String pathToStopWOrds() {
        return INPUT_PATH + "stopwords.txt";
    }

    public static String pathToTestSet() {
        return INPUT_PATH + "test.tab";
    }



    public static String pathToCorpus() {
        return OUTPUT_PATH + "corpus";
    }


    public static String pathToConditionals() {
        return OUTPUT_PATH + "vocabulary";
    }

    public static String pathToSums() {
        return OUTPUT_PATH + "sums";
    }


}
