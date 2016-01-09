package de.tub.dima.impro3.lda;

import org.apache.commons.math.special.Gamma;
import org.apache.flink.ml.math.DenseMatrix;
import org.apache.flink.ml.math.DenseVector;

/**
 * Created by arbuzinside on 8.1.2016.
 */
public class LDAUtils {




    public static DenseMatrix dirichletExpectation(DenseMatrix matrix) {

        return new DenseMatrix(matrix.numRows(),matrix.numCols(), dirichletExpectation(matrix.data()));
    }


    /**
     * TODO: check if the exp works for matrices
     *
     */


    public static DenseVector dirichletExpectation(DenseVector vector) {
        return new DenseVector(dirichletExpectation(vector.data()));
    }


    static double[] dirichletExpectation(double[] array) {
         double sum=0;
            for(double d: array){
                    sum += d;
        }
            double d = Gamma.digamma(sum);
             double[] result = new double[array.length];
        for(int i=0; i<array.length;++i){
            result[i] = Gamma.digamma(array[i]) - d;
        }
        return result;

    }

    private static double[] exp(double[] array) {
        int nc = array.length;
        double[] result = new double[nc];
        for (int k = 0; k < nc; ++k) {
            result[k] = Math.exp(array[k]);
        }
        return result;
    }



    public static DenseVector exp(DenseVector vector) {
        return new DenseVector(exp(vector.data()));
    }

    public static DenseMatrix exp(DenseMatrix matrix) {
        return new DenseMatrix(matrix.numRows(), matrix.numCols(),exp(matrix.data()));
    }




}
