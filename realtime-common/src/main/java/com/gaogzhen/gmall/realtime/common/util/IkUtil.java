package com.gaogzhen.gmall.realtime.common.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yhm
 * @create 2023-12-27 9:07
 */
public class IkUtil {

    public static List<String> IKSplit(String keywords){
        StringReader stringReader = new StringReader(keywords);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);
        ArrayList<String> result = new ArrayList<>();
        try {
            Lexeme next = ikSegmenter.next();
            while (next != null){
                result.add(next.getLexemeText());
                next =  ikSegmenter.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void main(String[] args) throws IOException {
        String s = "Apple 苹果15 iPhone 15 支持移动联通电信5G双卡双待手机 粉色 128GB";
        StringReader stringReader = new StringReader(s);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);
        Lexeme next = ikSegmenter.next();
        while (next != null){
            System.out.println(next.getLexemeText());
            next =  ikSegmenter.next();
        }
    }
}
