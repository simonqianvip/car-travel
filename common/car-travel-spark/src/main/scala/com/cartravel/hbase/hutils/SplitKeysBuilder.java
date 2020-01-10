package com.cartravel.hbase.hutils;

import com.google.inject.internal.util.$SourceProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Created by angel
 */
public class SplitKeysBuilder implements SplitKeyCalulaor{
    @Override
    public byte[][] getSplitKeys(int regionNum) {
        //1、构建前缀
        String[] keys = new String[regionNum] ;
        for(int i=0 ; i< regionNum;i++){
            String  pre = StringUtils.leftPad(String.valueOf(i) , 4 ,  "0") + "|";
            keys[i] = pre ;
        }
        System.out.println(keys);

        //2构建 一个返回的数组
        byte[][] splitKeys = new byte[keys.length][];

        //使用一个有序的集合 ， 封装前缀
        TreeSet<byte[]> row = new TreeSet<byte[]>(
//                new Comparator<byte[]>() {
//            @Override
//            public int compare(byte[] o1, byte[] o2) {
//                return String.valueOf(o1).compareTo(String.valueOf(o2));
//            }
//        }
                Bytes.BYTES_COMPARATOR
        );
        for(int i=0 ; i<keys.length;i++){
            row.add(Bytes.toBytes(keys[i]));
        }

        //迭代TreeSet ， 把值赋值给splitKeys
        final Iterator<byte[]> iterator = row.iterator();
        int i = 0 ;
        while (iterator.hasNext()){
            final byte[] tempRow = iterator.next();
            iterator.remove();
            splitKeys[i] = tempRow ;
            i++ ;
        }

        row.clear();
        row = null ;
        return splitKeys ;


    }
}
class Test_splitKeysBuilder{
    public static void main(String[] args) {
        SplitKeysBuilder splitKeysBuilder = new SplitKeysBuilder();
        byte[][] splitKeys = splitKeysBuilder.getSplitKeys(6);
        for(byte[] a:splitKeys){
            for(byte b : a){
                System.out.println(String.valueOf(b));
            }
            System.out.println();
        }
    }
}
