package de.tub.dima.impro3.lda;

import org.apache.commons.math.special.Gamma;
import org.apache.flink.ml.math.DenseMatrix;
import org.apache.flink.ml.math.DenseVector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by arbuzinside on 8.1.2016.
 */
public class LDAUtils {









    public static DenseVector dirichletExpectation(DenseVector vector) {
        return new DenseVector(dirichletExpectation(vector.data()));
    }






    public static DenseMatrix extractRows(DenseMatrix matrix, List<Integer> rows){



        DenseMatrix output = DenseMatrix.zeros(rows.size(), matrix.numCols());

        for (int i = 0; i < rows.size(); i ++)
            for(int j = 0; j < matrix.numCols(); j++)
                output.update(i, j, matrix.apply(i, j));



        return output;

    }






    public static DenseMatrix addToMatrix(DenseMatrix matrix, double value) {

        for (int r = 0; r < matrix.numRows(); r++) {
            for (int c = 0; c < matrix.numCols(); ++c) {
                matrix.update(r, c, matrix.apply(r,c) + value);
            }
        }
        return matrix;
    }




    public static DenseMatrix product(DenseMatrix matrix, double value) {


        for (int r = 0; r < matrix.numRows(); r++) {
            for (int c = 0; c < matrix.numCols(); ++c) {
                matrix.update(r, c, matrix.apply(r,c) * value);
            }
        }
        return matrix;
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







    public static DenseVector dot(DenseMatrix m, DenseVector v) {


        if (v.size() != m.numCols())
            throw new ArrayIndexOutOfBoundsException("wrong size");


        double [] result = new double[m.numRows()];

        for (int i = 0; i < m.numRows(); i++)
            for(int j = 0; j < v.size(); j ++)
                result[i] += m.apply(i, j) * v.apply(j);


        return new DenseVector(result);

    }







    public static DenseMatrix transpose(DenseMatrix mtx) {

        int numRows = mtx.numRows();
        int numCols = mtx.numCols();


        DenseMatrix output = DenseMatrix.zeros(mtx.numCols(), mtx.numRows());


        for (int c = 0 ; c < numRows ; c++)
            for (int d = 0 ; d < numCols ; d++)
                output.update(d,c, mtx.apply(c,d));




        return output;


    }







    public static DenseMatrix outer(DenseVector a, DenseVector b){


        DenseMatrix out = DenseMatrix.zeros(a.size(), b.size());

        for(int i=0; i< a.size(); ++i){
            for(int j=0; j<b.size();++j){
                out.update(i,j, a.apply(i) * b.apply(j));
            }
        }
        return out ;


    }







    public static DenseMatrix sum(DenseMatrix m1, DenseMatrix m2) {

        double[] result = new double[m1.numRows()*m2.numCols()];

        for(int i = 0; i < result.length; i++)
            result[i] = m1.data()[i] + m2.data()[i];


        return new DenseMatrix(m1.numRows(), m2.numCols(), result);

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



    /**
     * Digamma function (the first derivative of the logarithm of the gamma function).
     * @param array   - variational parameter
     * @return
     */
    static DenseMatrix dirichletExpectation(DenseMatrix matrix) {


        int numRows = matrix.numRows();
        int numCols = matrix.numCols();

        double[] vector = new double[numRows];
        Arrays.fill(vector, 0.0);

        for (int k = 0; k < numRows; ++k) {
            for (int w = 0; w < numCols; ++w) {
                try{
                    vector[k] += matrix.apply(k, w);
                }catch (Exception e){
                    throw new RuntimeException(e);
                }
            }
        }
        for (int k = 0; k < numRows; ++k) {
            vector[k] = Gamma.digamma(vector[k]);
        }

        DenseMatrix output = DenseMatrix.zeros(numRows, numCols);


        for (int k = 0; k < numRows; ++k) {
            for (int w = 0; w < numCols; ++w) {
                double z =  Gamma.digamma(matrix.apply(k,w));
                output.update(k,w, z - vector[k]);
            }
        }
        return output;
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


        int nc = matrix.numCols();
        int nr = matrix.numRows();


        DenseMatrix output = DenseMatrix.zeros(nr, nc);

        for (int k = 0; k < nr; ++k) {
            for (int w = 0; w < nc; ++w) {
                output.update(k, w, Math.exp(matrix.apply(k,w)));
            }
        }
        return output;
    }



}
