package com.atguigu.ct.analysis;

import com.atguigu.ct.analysis.tool.AnalysisBeanTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 分析数据
 */
public class AnalysisData {
    public static void main(String[] args) throws Exception{
//        ToolRunner.run(new AnalysisTextTool(), args);
        ToolRunner.run(new AnalysisBeanTool(), args);
    }
}
