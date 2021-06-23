import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.*;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.ArrayList;
import  java.util.Date;
import java.util.Calendar;

public class main {
    public static void main(String[] args) throws IOException {

//        final String htmlDir = args[0];
//        final int yearS = Integer.parseInt(args[1]);
//        final int monthS = Integer.parseInt(args[2]);
//        final int dayS = Integer.parseInt(args[3]);
//        final int yearE = Integer.parseInt(args[4]);
//        final int monthE = Integer.parseInt(args[5]);
//        final int dayE = Integer.parseInt(args[6]);

        final int yearS = 2020;
        final int monthS = 12;
        final int dayS = 03;
        final int yearE = 2020;
        final int monthE = 12;
        final int dayE = 04;


        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // convertHtmlToCSV2("/home/hamid/queryLog/2018_6_1.html");

        LocalDate start = LocalDate.of(yearS, monthS, dayS);
        LocalDate end = LocalDate.of(yearE, monthE, dayE);
        System.out.println("=========================================================================================");
        System.out.println("Starting to get sql query log from " + yearS + "-" + monthS + "-" + dayS + " to " + yearE + "-" + monthE + "-" + dayE);
        System.out.println("HTML files per each day is saved in " + "/Users/falker/Documents/Projects/skyserver/querylogs/perDay/");
        System.out.println("\n\n\n");

       // getAggQuery(start, end, htmlDir);

        for (LocalDate date = start; date.isBefore(end); date = date.plusDays(1)) {
            //    select count(*) from SqlLog
//where (statement like '%count(%' or statement like '%sum(%' or statement like '%avg(%' or statement like '%distinct(%' or statement like '%max(%' or statement like '%min(%' or statement like '%count (%' or statement like '%sum (%' or statement like '%avg (%' or statement like '%distinct (%' or statement like '%max (%' or statement like '%min (%') and (yy=2018)
            int day = date.getDayOfMonth();
            int month = date.getMonthValue();
            int year = date.getYear();
            HttpHandler httpHandler = new HttpHandler(year, month, day, "/Users/falker/Documents/Projects/skyserver/querylogs/perDay/");
            httpHandler.downloadResult();
            convertHtmlToCSV2("/Users/falker/Documents/Projects/skyserver/querylogs/perDay/" + String.valueOf(year) + "_" + String.valueOf(month) + "_" + String.valueOf(day) + ".html");

            // convertHtmlToCSV(httpHandler.downloadResult());
        }


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
        output += (doc.body().childNode(2).childNode(1).childNode(0).childNode(0).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(2).childNode(1).childNode(0).childNode(1).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(2).childNode(1).childNode(0).childNode(2).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(2).childNode(1).childNode(0).childNode(3).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(2).childNode(1).childNode(0).childNode(4).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(2).childNode(1).childNode(0).childNode(5).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(2).childNode(1).childNode(0).childNode(6).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(2).childNode(1).childNode(0).childNode(7).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(2).childNode(1).childNode(0).childNode(8).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(2).childNode(1).childNode(0).childNode(9).childNode(0).childNode(0).toString() + '\n');
        System.out.println("number of queries in file:" + outPath + " is " + doc.body().childNode(2).childNode(1).childNodeSize());
        //   doc.body().childNode(2).childNode(1);
        for (int i = 1; i < doc.body().childNode(2).childNode(1).childNodeSize(); i++) {
            if (doc.body().childNode(2).childNode(1).childNode(i).childNodeSize() != 0
                    && doc.body().childNode(2).childNode(1).childNode(i).childNodeSize() != 10) {
                System.out.println(String.valueOf(i) + "::" + doc.body().childNode(2).childNode(1).childNode(i));
                continue;
            }
            if (doc.body().childNode(2).childNode(1).childNode(i).childNodeSize() == 0 || doc.body().childNode(2).childNode(1).childNode(i).childNode(9).childNode(0).childNodeSize() == 0)
                continue;
            output += (doc.body().childNode(2).childNode(1).childNode(i).childNode(0).childNode(0).childNode(0).toString() + ';');
            output += (doc.body().childNode(2).childNode(1).childNode(i).childNode(1).childNode(0).childNode(0).toString() + ';');
            output += (doc.body().childNode(2).childNode(1).childNode(i).childNode(2).childNode(0).childNode(0).toString() + ';');
            output += (doc.body().childNode(2).childNode(1).childNode(i).childNode(3).childNode(0).childNode(0).toString() + ';');
            output += (doc.body().childNode(2).childNode(1).childNode(i).childNode(4).childNode(0).childNode(0).toString() + ';');
            output += (doc.body().childNode(2).childNode(1).childNode(i).childNode(5).childNode(0).childNode(0).toString() + ';');
            output += (doc.body().childNode(2).childNode(1).childNode(i).childNode(6).childNode(0).childNode(0).toString() + ';');
            output += (doc.body().childNode(2).childNode(1).childNode(i).childNode(7).childNode(0).childNode(0).toString() + ';');
            output += (doc.body().childNode(2).childNode(1).childNode(i).childNode(8).childNode(0).childNode(0).toString() + ';');

            if (doc.body().childNode(2).childNode(1).childNode(i).childNode(9).childNode(0).childNodeSize() == 3)
                output += (doc.body().childNode(2).childNode(1).childNode(i).childNode(9).childNode(0).childNode(0).toString() + " " + doc.body().childNode(2).childNode(1).childNode(i).childNode(9).childNode(0).childNode(1).toString() + " " + doc.body().childNode(2).childNode(1).childNode(i).childNode(9).childNode(0).childNode(2).toString() + '\n');
            else
                output += (doc.body().childNode(2).childNode(1).childNode(i).childNode(9).childNode(0).childNode(0).toString() + '\n');
            outQueries++;
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

    private static void convertHtmlToCSV(String path) throws IOException {
        int outQueries = 0;
        String outPath = path.substring(0, path.indexOf('.')) + ".csv";
        File file = new File(outPath);
        Files.deleteIfExists(file.toPath());
        PrintWriter out = new PrintWriter(outPath);
        File input = new File(path);
        Document doc = Jsoup.parse(input, "UTF-8", "http://example.com/");
        String output = "";
        output += (doc.body().childNode(1).childNode(0).childNode(0).childNode(0).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(1).childNode(0).childNode(0).childNode(1).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(1).childNode(0).childNode(0).childNode(2).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(1).childNode(0).childNode(0).childNode(3).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(1).childNode(0).childNode(0).childNode(4).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(1).childNode(0).childNode(0).childNode(5).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(1).childNode(0).childNode(0).childNode(6).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(1).childNode(0).childNode(0).childNode(9).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(1).childNode(0).childNode(0).childNode(16).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(1).childNode(0).childNode(0).childNode(17).childNode(0).childNode(0).toString() + ';');
        output += (doc.body().childNode(1).childNode(0).childNode(0).childNode(18).childNode(0).childNode(0).toString() + '\n');
        System.out.println("number of queries in file:" + outPath + " is " + doc.body().childNode(1).childNode(0).childNodeSize());
        for (int i = 1; i < doc.body().childNode(1).childNode(0).childNodeSize(); i++) {
            if (doc.body().childNode(1).childNode(0).childNode(i).childNodeSize() != 21 || doc.body().childNode(1).childNode(0).childNode(i).childNode(17).childNode(0).childNodeSize() != 1 || doc.body().childNode(1).childNode(0).childNode(i).childNode(18).childNodeSize() != 1) {
                //      System.err.println(doc.body().childNode(1).childNode(0).childNode(i).childNode(17));
                continue;
            }
            output += (doc.body().childNode(1).childNode(0).childNode(i).childNode(0).childNode(0).childNode(0).toString() + ';');
            output += (doc.body().childNode(1).childNode(0).childNode(i).childNode(1).childNode(0).childNode(0).toString() + ';');
            output += (doc.body().childNode(1).childNode(0).childNode(i).childNode(2).childNode(0).childNode(0).toString() + ';');
            output += (doc.body().childNode(1).childNode(0).childNode(i).childNode(3).childNode(0).childNode(0).toString() + ';');
            output += (doc.body().childNode(1).childNode(0).childNode(i).childNode(4).childNode(0).childNode(0).toString() + ';');
            output += (doc.body().childNode(1).childNode(0).childNode(i).childNode(5).childNode(0).childNode(0).toString() + ';');
            output += (doc.body().childNode(1).childNode(0).childNode(i).childNode(6).childNode(0).childNode(0).toString() + ';');
            output += (doc.body().childNode(1).childNode(0).childNode(i).childNode(9).childNode(0).childNode(0).toString() + ';');
            output += (doc.body().childNode(1).childNode(0).childNode(i).childNode(16).childNode(0).childNode(0).toString() + ';');

            output += (doc.body().childNode(1).childNode(0).childNode(i).childNode(17).childNode(0).childNode(0).toString() + ';');
            output += (doc.body().childNode(1).childNode(0).childNode(i).childNode(18).childNode(0).childNode(0).toString() + '\n');
            outQueries++;
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

    private static void getAggQuery(LocalDate start, LocalDate end, String htmlDir) throws IOException {
        String outPath = htmlDir + "/" + start.toString() + "_" + end.toString() + "AggQ.csv";
        File file = new File(outPath);
        Files.deleteIfExists(file.toPath());
        PrintWriter out = new PrintWriter(outPath);
        out.println("yy;mm;dd;hh;mi;ss;seq;clientIP;rows;statement");

        for (LocalDate date = start; date.isBefore(end); date = date.plusDays(1)) {
            int day = date.getDayOfMonth();
            int month = date.getMonthValue();
            int year = date.getYear();
            File input = new File(htmlDir + '/' + year + '_' + month + '_' + day + ".csv");
            BufferedReader reader = new BufferedReader(new FileReader(input));
            reader.readLine();

            String line = reader.readLine();
            while (line != null) {
                if (line.contains("count(") || line.contains("count (") || line.contains("avg(") || line.contains("avg (")
                        || line.contains("sum(") || line.contains("sum (") || line.contains("distinct(")
                        || line.contains("distinct (") || line.contains("min(") || line.contains("min (")
                        || line.contains("max(") || line.contains("max ("))
                    out.println(line);
                line = reader.readLine();
            }
            reader.close();

        }
    }
}
