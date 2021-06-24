import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.TextNode;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.Arrays;

public class convertHtmlQueryToCSV {
    public static void main(String[] args) throws IOException {
        convertHtmlToCSV2("/home/hamid/Desktop/2013.html");

    }

    private static void convertHtmlToCSV2(String path) throws IOException {
        int outQueries = 0;
        String outPath = path.substring(0, path.indexOf('.')) + ".csv";
        File file = new File(outPath);
        Files.deleteIfExists(file.toPath());
        PrintWriter out = new PrintWriter(outPath);
        File input = new File(path);
        Document doc = Jsoup.parse(input, "UTF-8", "http://example.com/");
        String output = "";
        int rowSize = doc.body().childNode(2).childNode(1).childNodes().size();
        for (int i = 2; i < rowSize; i = i + 2) {
            output += doc.body().childNode(2).childNode(1).childNode(i).childNode(0).childNode(0).childNode(0).toString() + ";";
            output += doc.body().childNode(2).childNode(1).childNode(i).childNode(1).childNode(0).childNode(0).toString() + ";";
            output += doc.body().childNode(2).childNode(1).childNode(i).childNode(2).childNode(0).childNode(0).toString() + ";";
            output += doc.body().childNode(2).childNode(1).childNode(i).childNode(3).childNode(0).childNode(0).toString() + ";";
            output += doc.body().childNode(2).childNode(1).childNode(i).childNode(4).childNode(0).childNode(0).toString() + ";";
            output += doc.body().childNode(2).childNode(1).childNode(i).childNode(5).childNode(0).childNode(0).toString() + ";";
            output += doc.body().childNode(2).childNode(1).childNode(i).childNode(6).childNode(0).childNode(0).toString() + ";";
            output += doc.body().childNode(2).childNode(1).childNode(i).childNode(7).childNode(0).childNode(0).toString() + ";";
            output += doc.body().childNode(2).childNode(1).childNode(i).childNode(8).childNode(0).childNode(0).toString() + ";";
            output += cleanSkyServerQuery(removeWrongKeywords(removeComment(((TextNode) doc.body().childNode(2).childNode(1).childNode(i).childNode(9).childNode(0).childNode(0)).getWholeText().toLowerCase()))) + "\n";
            if (output.length() > 500) {
                out.print(output);
                output = "";
            }
        }
        out.print(output);
        out.close();
        System.out.println("number of valid queries is " + outQueries);
        System.out.println();
    }

    public static String removeComment(String query) {
        String out = "";
        String[] temp = query.split("\n");
        for (int i = 0; i < temp.length; i++) {
            int index = temp[i].indexOf("--");
            if (index >= 0)
                out += " " + temp[i].substring(0, index);
            else
                out += " " + temp[i];
        }
        return out;
    }

    public static String removeWrongKeywords(String query) {
        return query.replace("&gt;", ">")
                .replace("&lt;", "<").replace("&amp;", "&").replace("[", " ")
                .replace("isnull(", " ").replace("]", " ").replace("distinct", " ")
                .replace(",0)", " ").replace("\"Star\"", "star").replace("bestdr8.dr8.", "")
                .replace("bestdr8..", "").replace("dr8.", "").replace("dr8.", "")
                .replace("dr10.", "").replace("dr9.", "").replace("set parseonly on", "").replace("bestdr9..", "");
    }
//        if (!l.contains(" CLASS=") && !l.contains("GalSpecLine") && !l.contains("&amp;") && !l.contains("sppParams")
//        && !l.contains("min(") && !l.contains("max(") && !l.contains("ZooSpec") && !l.contains("emissionLinesPort")
//        && !l.contains("count(1)") && !l.contains("galSpecLine") && !l.contains("emissionlinesport")
//        && !l.contains("galspecline") && !l.contains("35\'") && !l.contains("cannonStar") && !l.contains("mydb")
//        && !l.contains("MYDB") && !l.contains("datalength") && !l.contains("Countof") && !l.contains("speclineall")
//        && !l.contains("SpecLineAll") && !l.contains("mangaDAPall") && !l.contains("mangadapall") && !l.contains("fPhotoFlags")
//        && !l.contains("fGet") && !l.contains("fGetNearbySpecObjEq") && !l.contains("--") && !l.contains("ring_galaxies_z")
//        && !l.contains("PrimTarget") && !l.contains("objTypeName") && !l.contains("fiberMag_r") && !l.contains("specclass")
//        && !l.contains("specClass")) {
    public static String cleanSkyServerQuery(String query) {
        String[] token = query.split(" ");
        if(token.length<4) {
            System.out.println(query);
            return "";
        }
        for (int i = 0; i < token.length; i++) {
            if (token[i].equals("as") && token[i + 1].contains("\'"))
                token[i + 1] = token[i + 1].replace("\'", "");
            if (token[i].equalsIgnoreCase("top")) {
                token[i] = "";
                token[i + 1] = "";
            }
            if (token[i].contains("mydb") || token[i].contains("spectroflux_r") || token[i].contains("photoobjall.type") || token[i].contains("fspectroflux_r") || token[i].contains("0x000000000000ffff")|| token[i].contains("specclass")|| token[i].contains("column1") )
                return "";
            if (token[i].equals("avg_dec") )
                token[i]= "avg_dec,";
            if (token[i].equals("selectcount"))
                token[i] = "select count";
            if (token[i].equals("order ra") && token[i+1].equals("by") && (token[i+2].equals("ra") ||token[i+2].equals("ra\""))) {
                token[i] = "";
                token[i+1] = "";
                token[i+2] = "";
            }
            if(token[i].equals("\"select"))
                token[i]="select";
            if (token[i].equals("count(*)from"))
                token[i] = "count(*) from";
            if (token[i].equals("count(1)from"))
                token[i] = "count(1) from";
            if (token[i].contains("fgetnearbyobjeq")) {
                token[i] = "photoobj";
            }
            if (token[i].equalsIgnoreCase("into") && token[i + 1].contains("db")) {
                token[i] = "";
                token[i + 1] = "";
            }
        }
        String out = "";
        for (int i = 0; i < token.length; i++)
            out += " " + token[i];
        return out.replace("stdev(", "avg(").replace("stdev (", "avg (").replaceAll("\\s{2,}", " ").trim();
    }
}
