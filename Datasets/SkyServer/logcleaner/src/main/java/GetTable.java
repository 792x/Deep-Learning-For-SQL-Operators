import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.util.ArrayList;

public class GetTable {
    private static final String PARENT_DIR = "/Users/falker/Documents/Projects/skyserver/data/";

    public static String getPathToSave(String table, String filename) {
        return PARENT_DIR + table + "/" + filename + ".csv";
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Starting GetTable...");
        String GET_URL = "http://skyserver.sdss.org/dr16/en/tools/search/x_results.aspx?searchtool=SQL&TaskName=Skyserver.Search.SQL&syntax=NoSyntax&ReturnHtml=true&";
        ArrayList<String> tableNames = new ArrayList<String>();
//        tableNames.add("PhotoObjAll"); //101
//        tableNames.add("GalaxyTag"); //101
//        tableNames.add("SpecObjAll"); //101
//        tableNames.add("star");  //101
//        tableNames.add("photoprimary"); //101
//        tableNames.add("Galaxy"); //101
//        tableNames.add("SpecObj"); //101
//        tableNames.add("photoObj"); //101
//        tableNames.add("Field"); //5999
//        tableNames.add("FIRST"); //5999
//        tableNames.add("SpecPhoto"); //5999
//        tableNames.add("SpecPhotoAll"); //5999
//        tableNames.add("sppparams"); //5999
//        tableNames.add("wise_xmatch"); //5999
//        tableNames.add("emissionlinesport"); //5999
//        tableNames.add("galspecline"); //5999
//        tableNames.add("photoz"); //5999
//        tableNames.add("wise_allsky"); // 13999
//        tableNames.add("zoospec");   //13999
//        tableNames.add("zoonospec");  //13999
//        tableNames.add("propermotions"); //5999
//        tableNames.add("stellarmassstarformingport"); //13999
//        tableNames.add("sdssebossfirefly"); //13999
//        tableNames.add("spplines"); //13999
//        tableNames.add("neighbors"); //-1
//        tableNames.add("specline"); //-1
//        tableNames.add("stellarmasspcawisc"); //-1
//        tableNames.add("photoprofile"); //-1
//        tableNames.add("masses"); //-1
//        tableNames.add("twomass"); //-1
//        tableNames.add("photoprofile"); //-1
        //counter<9000=>8999
        //counter<11000=>10999
        //counter<13000=>12999
        //counter<14000=>13999
        tableNames.add("apogeeDesign");
        tableNames.add("apogeeField");
        tableNames.add("apogeeObject");
        tableNames.add("apogeePlate");
        tableNames.add("apogeeStar");
        tableNames.add("apogeeStarAllVisit");
        tableNames.add("apogeeStarVisit");
        tableNames.add("apogeeVisit");
        tableNames.add("aspcapStar");
        tableNames.add("aspcapStarCovar");
        tableNames.add("AtlasOutline");
        tableNames.add("cannonStar");
        tableNames.add("DataConstants");
        tableNames.add("DBColumns");
        tableNames.add("DBObjects");
        tableNames.add("DBViewCols");
        tableNames.add("Dependency");
        tableNames.add("detectionIndex");
        tableNames.add("Diagnostics");
        tableNames.add("emissionLinesPort");
        tableNames.add("Field");
        tableNames.add("FieldProfile");
        tableNames.add("FileGroupMap");
        tableNames.add("FIRST");
        tableNames.add("Frame");
        tableNames.add("galSpecExtra");
        tableNames.add("galSpecIndx");
        tableNames.add("galSpecInfo");
        tableNames.add("galSpecLine");
        tableNames.add("HalfSpace");
        tableNames.add("History");
        tableNames.add("IndexMap");
        tableNames.add("Inventory");
        tableNames.add("LoadHistory");
        tableNames.add("mangaAlfalfaDR15");
        tableNames.add("mangaDAPall");
        tableNames.add("mangaDRPall");
        tableNames.add("mangaFirefly");
        tableNames.add("mangaGalaxyZoo");
        tableNames.add("mangaHIall");
        tableNames.add("mangaHIbonus");
        tableNames.add("mangaPipe3D");
        tableNames.add("mangatarget");
        tableNames.add("marvelsStar");
        tableNames.add("marvelsVelocityCurveUF1D");
        tableNames.add("Mask");
        tableNames.add("MaskedObject");
        tableNames.add("mastar_goodstars");
        tableNames.add("mastar_goodvisits");
        tableNames.add("Neighbors");
        tableNames.add("nsatlas");
        tableNames.add("PartitionMap");
        tableNames.add("PawlikMorph");
        tableNames.add("PhotoObjAll");
        tableNames.add("PhotoObjDR7");
        tableNames.add("PhotoPrimaryDR7");
        tableNames.add("PhotoProfile");
        tableNames.add("Photoz");
        tableNames.add("PhotozErrorMap");
        tableNames.add("Plate2Target");
        tableNames.add("PlateX");
        tableNames.add("ProfileDefs");
        tableNames.add("ProperMotions");
        tableNames.add("PubHistory");
        tableNames.add("qsoVarPTF");
        tableNames.add("qsoVarStripe");
        tableNames.add("RC3");
        tableNames.add("RecentQueries");
        tableNames.add("Region");
        tableNames.add("Region2Box");
        tableNames.add("RegionArcs");
        tableNames.add("RegionPatch");
        tableNames.add("RegionTypes");
        tableNames.add("Rmatrix");
        tableNames.add("ROSAT");
        tableNames.add("Run");
        tableNames.add("RunShift");
        tableNames.add("sdssBestTarget2Sector");
        tableNames.add("SDSSConstants");
        tableNames.add("sdssEbossFirefly");
        tableNames.add("sdssImagingHalfSpaces");
        tableNames.add("sdssPolygon2Field");
        tableNames.add("sdssPolygons");
        tableNames.add("sdssSector");
        tableNames.add("sdssSector2Tile");
        tableNames.add("sdssTargetParam");
        tableNames.add("sdssTileAll");
        tableNames.add("sdssTiledTargetAll");
        tableNames.add("sdssTilingGeometry");
        tableNames.add("sdssTilingInfo");
        tableNames.add("sdssTilingRun");
        tableNames.add("segueTargetAll");
        tableNames.add("SiteConstants");
        tableNames.add("SiteDBs");
        tableNames.add("SiteDiagnostics");
        tableNames.add("SkipFinishPhases");
        tableNames.add("SpecDR7");
        tableNames.add("SpecObjAll");
        tableNames.add("SpecPhotoAll");
        tableNames.add("spiders_quasar");
        tableNames.add("sppLines");
        tableNames.add("sppParams");
        tableNames.add("sppTargets");
        tableNames.add("stellarMassFSPSGranEarlyDust");
        tableNames.add("stellarMassFSPSGranEarlyNoDust");
        tableNames.add("stellarMassFSPSGranWideDust");
        tableNames.add("stellarMassFSPSGranWideNoDust");
        tableNames.add("stellarMassPassivePort");
        tableNames.add("stellarMassPCAWiscBC03");
        tableNames.add("stellarMassPCAWiscM11");
        tableNames.add("stellarMassStarformingPort");
        tableNames.add("StripeDefs");
        tableNames.add("Target");
        tableNames.add("TargetInfo");
        tableNames.add("thingIndex");
        tableNames.add("TwoMass");
        tableNames.add("TwoMassXSC");
        tableNames.add("USNO");
        tableNames.add("Versions");
        tableNames.add("WISE_allsky");
        tableNames.add("WISE_xmatch");
        tableNames.add("wiseForcedTarget");
        tableNames.add("Zone");
        tableNames.add("zoo2MainPhotoz");
        tableNames.add("zoo2MainSpecz");
        tableNames.add("zoo2Stripe82Coadd1");
        tableNames.add("zoo2Stripe82Coadd2");
        tableNames.add("zoo2Stripe82Normal");
        tableNames.add("zooConfidence");
        tableNames.add("zooMirrorBias");
        tableNames.add("zooMonochromeBias");
        tableNames.add("zooNoSpec");
        tableNames.add("zooSpec");
        tableNames.add("zooVotes");


        // Create directories if needed

        tableNames.forEach(dir -> {
            File directory = new File(PARENT_DIR + dir);
            if (!directory.exists()) {
                System.out.println("Creating new directory " + dir);
                directory.mkdir();
            }
        });

        int counter = 100;

        for (String tableName : tableNames) {
            String PARAMS = "cmd=SELECT+*+FROM+" + tableName + "+WHERE+objID+%25+99999>12999+and+objID+%25+99999<" + counter + "&format=csv&tableName=";
            //String PARAMS="cmd=SELECT+*+FROM+" + tableNames.get(i) + "+WHERE+objID+%25+99999=" + counter + "&format=csv&tableName=";
            if (tableName.equals("Field"))
                PARAMS = "cmd=SELECT+*+FROM+" + tableName + "+WHERE+fieldID+%25+99999>12999+and+fieldID+%25+99999<" + counter + "&format=csv&tableName=";
            //PARAMS="cmd=SELECT+*+FROM+" + tableNames.get(i) + "+WHERE+fieldID+%25+99999=" + counter + "&format=csv&tableName=";
            if (tableName.equals("SpecObjAll") || tableName.equals("specObjAll") || tableName.equals("SpecObj")
                    || tableName.equals("specObj") || tableName.equals("SpecPhoto") || tableName.equals("emissionlinesport")
                    || tableName.equals("galspecline") || tableName.equals("zoospec") || tableName.equals("zoonospec") || tableName.equals("stellarmassstarformingport"))
                PARAMS = "cmd=SELECT+*+FROM+" + tableName + "+WHERE+specObjID+%25+99999>12999+and+specObjID+%25+99999<" + counter + "&format=csv&tableName=";
            //PARAMS="cmd=SELECT+*+FROM+" + tableNames.get(i) + "+WHERE+specObjID+%25+99999=" + counter + "&format=csv&tableName=";
            if (tableName.equals("sppparams") || tableName.equals("sdssebossfirefly") || tableName.equals("spplines"))
                PARAMS = "cmd=SELECT+*+FROM+" + tableName + "+WHERE+SPECOBJID+%25+99999>12999+and+SPECOBJID+%25+99999<" + counter + "&format=csv&tableName=";
            //PARAMS="cmd=SELECT+*+FROM+" + tableNames.get(i) + "+WHERE+SPECOBJID+%25+99999=" + counter + "&format=csv&tableName=";
            if (tableName.equals("wise_xmatch"))
                PARAMS = "cmd=SELECT+*+FROM+" + tableName + "+WHERE+sdss_objid+%25+99999>12999+and+sdss_objid+%25+99999<" + counter + "&format=csv&tableName=";
            //PARAMS="cmd=SELECT+*+FROM+" + tableNames.get(i) + "+WHERE+sdss_objid+%25+99999=" + counter + "&format=csv&tableName=";
            if (tableName.equals("wise_allsky"))
                PARAMS = "cmd=SELECT+*+FROM+" + tableName + "+WHERE+cntr+%25+99999>12999+and+cntr+%25+99999<" + counter + "&format=csv&tableName=";
            //PARAMS="cmd=SELECT+*+FROM+" + tableNames.get(i) + "+WHERE+cntr+%25+99999=" + counter + "&format=csv&tableName=";

            URL url = new URL(GET_URL + PARAMS);
            System.out.println("Requesting url:" + url);
            URLConnection con2 = url.openConnection();
            InputStream is = con2.getInputStream();

            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            System.out.println("Writing to disk...");
            String line = null;
            File file = new File(getPathToSave(tableName, tableName));
            Files.deleteIfExists(file.toPath());
            PrintWriter out = new PrintWriter(getPathToSave(tableName, tableName));
            br.readLine();
            int numLines = 0;
            while ((line = br.readLine()) != null) {
                out.println(line);
                numLines++;
            }
            System.out.println("Finished, total lines: " + numLines);
            br.close();
            out.close();
        }
    }
}
