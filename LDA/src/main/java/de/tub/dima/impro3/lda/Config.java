package de.tub.dima.impro3.lda;

/**
 * Created by arbuzinside on 7.2.2016.
 */
public class Config {
//
//    private static final String INPUT_PATH = "";
//    private static final String OUTPUT_PATH = "";

    //
    private static final String INPUT_PATH = "src/resources/";
    private static final String OUTPUT_PATH = "src/results/";

    //  private static final String INPUT_PATH = "hdfs:///LDA_DATA/wikiDataSet4/";
    //private static final String OUTPUT_PATH = "hdfs:///LDA_DATA/wikiDataSet4/";

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
        return OUTPUT_PATH + "corpus1";
    }


    public static String pathToConditionals() {
        return OUTPUT_PATH + "vocabulary";
    }

    public static String pathToSums() {
        return OUTPUT_PATH + "sums";
    }

    public static String pathToSparkMatrix() {
        return OUTPUT_PATH + "sparkMatrix";
    }

    public static String pathToFlinkMatrix() {
        return OUTPUT_PATH + "flinkMatrix";
    }

    public static String pathToOutFlink() {
        return OUTPUT_PATH + "flinkResult";
    }

    public static String pathToOutSpark() {
        return OUTPUT_PATH + "sparkResult";
    }

    public static String pathToWikiPages() {
        return OUTPUT_PATH + "wiki";
    }

    public static String pathToWikiResults() {
        return OUTPUT_PATH + "wikiResult";
    }
}
