package de.tub.dima.impro3.lda;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math.special.Gamma;
import org.apache.flink.ml.math.DenseMatrix;
import org.apache.flink.ml.math.DenseVector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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


    public static DenseMatrix extractColumns(DenseMatrix matrix, List<Integer> rows){

        ArrayList<Double> betaElements = new ArrayList<>(rows.size()* matrix.numCols());

        while (rows.iterator().hasNext()){

            int currentRow = rows.iterator().next();

            for (int i = 0; i < matrix.numCols(); i ++){
                betaElements.add(matrix.apply(currentRow, i));
            }
        }


        Double[] array = new Double[rows.size()*matrix.numCols()];
        betaElements.toArray(array);

        return new DenseMatrix(rows.size(), matrix.numCols(), ArrayUtils.toPrimitive(array));

    }



    public static double[][] vectorToMatrix(double[] data, int rows, int cols){


        double[][] result = new double[rows][cols];

        for(int i=0; i < rows; i ++)
            for(int j = 0; j < cols; j++)
                result[i][j] = data[i+j];


        return result;

    }


    public static DenseVector addToVector(DenseVector vector, double value) {
        double[]  result = new double[vector.size()];
        for (int c = 0; c < vector.size(); ++c) {
            result [c] = vector.data()[c] + value;
        }
        return new DenseVector(result);
    }


    public static DenseVector divideVectors(DenseVector v1, DenseVector v2) {
        if(v1.size() != v2.size()){
            throw new IllegalArgumentException("Vectors are not aligned");
        }
        double[] result  = new double[v1.size()];
        for (int i = 0; i < result.length; ++i) {
            result[i] =   v1.data()[i]/v2.data()[i];
        }
        return new DenseVector(result);
    }

    public static DenseVector dot(double[] m1, double[][] m2) {
        if(m1.length != m2 .length){
            throw new IllegalArgumentException("Length of vector " + m1.length
                    + " does not match the number of matrix rows " + m2 .length);
        }
        double [] result = new double[m2[0].length];
        Arrays.fill(result, 0d);

        for (int c = 0; c < m2[0].length; ++c) {
            for(int i=0; i< m1.length;++i){
                result[c] += m1[i] *m2[i][c];
            }
        }
        double [] app =  new double[m2[0].length];

        for(int i=0; i< m2[0].length;++i){
            app[i] = result[i];
        }
        return new DenseVector(app);
    }

    //TODO: optimize, it's bullshit
    public static double[][] transpose(DenseMatrix mtx) {

        int numRows = mtx.numRows();
        int numCols = mtx.numCols();


        double[][] matrix = new double[numRows][numCols];

        for(int i=0; i < numRows; i ++)
            for(int j = 0; j < numCols; j++)
                matrix[i][j] = mtx.data()[i+j];


        double[][] result = new double[numCols][];
        for (int r = 0; r < numCols; ++r) {
            result[r] = new double[numRows];
            for (int c = 0; c < numRows; ++c) {
                result[r][c] = matrix[c][r];
            }
        }

      return result;
    }


    public static DenseVector product(DenseVector v1, DenseVector v2) {
        if (v1.size() != v2.size()) {
            throw new IllegalArgumentException("Vectors are not of the same size");
        }
        double[] result = new double[v1.size()];
        for (int i = 0; i < result.length; ++i) {
            result[i] = v1.data()[i] * v2.data()[i];
        }
        return new DenseVector(result);
    }


   public static double[] dirichletExpectation(double[] array) {
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


    public static boolean closeTo(DenseVector initial, DenseVector other, double within){
        if(initial.size() !=other.size()){
            throw new  IllegalArgumentException();
        }
        double eps= 0d;
        for(int i=0; i< other.size();++i){
            eps += Math.abs(initial.data()[i] - other.data()[i]);
        }
        eps/=initial.size();
        return eps<within;
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
