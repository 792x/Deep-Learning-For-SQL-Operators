import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;

public class HttpHandler {
    private String USER_AGENT = "Mozilla/5.0";
    private String GET_URL = "http://skyserver.sdss.org/log/en/traffic/x_sql.asp?";
    private String OUTPUT_DIR = "";
    private int month = 0;
    private int year = 0;
    private int day=0;
    private String  PARAMS = "cmd=SELECT+yy,mm,dd,hh,mi,ss,seq,clientIP,rows,statement+FROM+SqlLog+WHERE+yy%3D" + year + "+and+mm%3D" + month + "+and+dd%3D" + day + "&format=html";
    private String  PARAMS2 = "cmd=select+count%28*%29+from+SqlLog%0D%0Awhere+statement+like+%27%25count%28%25%27+or+statement+like+%27%25sum%28%25%27+or+statement+like+%27%25avg%28%25%27+or+statement+like+%27%25distinct%28%25%27+or+statement+like+%27%25max%28%25%27+or+statement+like+%27%25min%28%25%27+or+statement+like+%27%25count+%28%25%27+or+statement+like+%27%25sum+%28%25%27+or+statement+like+%27%25avg+%28%25%27+or+statement+like+%27%25distinct+%28%25%27+or+statement+like+%27%25max+%28%25%27+or+statement+like+%27%25min+%28%25%27+and+yy%3D" + year + "+and+mm%3D"+month+"&format=html";
    PrintWriter out;
    ////////////////////////////////////////////////////////////////////////////////////////////


    public HttpHandler(int year, int month,int day, String OUTPUT_DIR) {
        this.month = month;
        this.year = year;
        this.day=day;
        this.OUTPUT_DIR = OUTPUT_DIR;
        resetParams();
    }


    public String downloadResult() throws IOException {
        URL url = new URL(GET_URL + PARAMS);
        URLConnection con2 = url.openConnection();
        InputStream is =con2.getInputStream();

        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        String line = null;
        File file = new File(getOutputPath());
        Files.deleteIfExists(file.toPath());
        out = new PrintWriter(getOutputPath());

        while ((line = br.readLine()) != null) {
            out.println(line);
        }

        br.close();
        out.close();
  /*      int numLine = 0;
        URL obj = new URL(GET_URL + PARAMS);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        File file = new File(getOutputPath());
        Files.deleteIfExists(file.toPath());
        out = new PrintWriter(getOutputPath());
        con.setRequestMethod("GET");
        con.setRequestProperty("User-Agent", USER_AGENT);
        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);




        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                numLine++;
                response.append(inputLine);
                if (response.length() > 500) {
                    out.print(response.toString());
                    response.delete(0, response.length());
                }
            }
            out.print(response.toString());
            out.close();
            in.close();
        } else {
            return "";
        }
        System.out.println("Saved " + numLine + " lines for query log of " + year + "-" + month + "-" + day + " as html file " + getOutputPath());*/
        return getOutputPath();
    }

    private void resetParams() {
        //%28statement+like+%22%25group+by%25%22+or+statement+like+%22%25distinct%25%22+or+statement+like+%22%25+join+%25%22%29+
        PARAMS = "cmd=SELECT+yy,mm,dd,hh,mi,ss,seq,clientIP,rows,statement%0D%0AFROM+SqlLog%0D%0AWHERE+yy%3D"+year+"+and+mm%3D" + month + "+and+dd%3D" + day + "&format=html";
    }

    private String getOutputPath() {
        return OUTPUT_DIR + '/' + year + '_' + month + '_' + day + ".html";
    }

    public String getUSER_AGENT() {
        return USER_AGENT;
    }

    public void setUSER_AGENT(String USER_AGENT) {
        this.USER_AGENT = USER_AGENT;
    }

    public String getGET_URL() {
        return GET_URL;
    }

    public void setGET_URL(String GET_URL) {
        this.GET_URL = GET_URL;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getRequstURL() {
        return GET_URL + PARAMS;
    }
}
