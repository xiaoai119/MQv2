import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
class Solution {
    public static void writeFile() {
        try {
            File writeName = new File("D:\\workspace\\build_dataset\\all.txt"); // 相对路径，如果没有则要建立一个新的output.txt文件
            writeName.createNewFile(); // 创建新文件,有同名的文件的话直接覆盖
            try (FileWriter writer = new FileWriter(writeName);
                 BufferedWriter out = new BufferedWriter(writer)
            ) {
                for(Integer i=1;i<401;i++){
                    String s="000000"+i.toString();
                    s = s.substring(s.length()-6,s.length());
                    out.write(s+"\r\n"); // \r\n即为换行
                }
                out.flush(); // 把缓存区内容压入文件

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Solution solution = new Solution();
        solution.writeFile();
    }
}