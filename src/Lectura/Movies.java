package Lectura;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Movies {
    public static String PATH="C:\\Users\\josemolina\\Downloads\\ml-25m\\ml-25m";

    public static void main(String [] args) throws IOException {
        String fileName = PATH+"\\movies.csv";
        FileReader fileReader = new FileReader(fileName);
        List<Integer> samplesSizes = Arrays.asList(10,100,1000, 10000, 50000, 62423 );
        List<String> records = new ArrayList<>();
        boolean exportFull=false;

        StringBuffer buffer = new StringBuffer();

        try (BufferedReader bufferedReader = new BufferedReader(fileReader))
        {
            String header="";
            String line;
            int lineas=0;
            while((line = bufferedReader.readLine()) != null) {
                if(lineas==0)
                    header=line;
                else
                    records.add(line);

                if(!exportFull)
                {
                    for (Integer valueSize: samplesSizes)
                    {
                        if(valueSize.equals(lineas))
                        {
                            writeRawJson(records, header);
                            writeRawSQL(records, header);
                        }
                    }
                }
                lineas++;
            }
            if(exportFull)
            {
                writeRawJson(records, header);
                writeRawSQL(records, header);
            }
            System.out.println("lineas = " + (lineas-1));
        }
    }
    private static void writeRawJson(List<String> records, String header) throws IOException {
        String filePath = PATH+"\\movies-json-" + (records.size()) + ".json";
        System.out.println("filePath = " + filePath);

        String[] headers = header.split(",");


        FileWriter writer = new FileWriter(filePath);
        BufferedWriter bufferedWriter = new BufferedWriter(writer);

        try {
            int index = 0;
            for (String record : records) {

                String[] localRecords = getRecords(record);
                String value = "{";

                for (int i = 0; i < headers.length; i++) {
                    value += "\"" + headers[i] + "\": " + getStringValue(localRecords[i]);

                    if (i < headers.length - 1)
                        value += ",";
                }

                if (index < records.size() - 1)
                    value += "},";
                else
                    value += "}";

                bufferedWriter.write(value+"\n");

                index++;
            }
        } finally {
            if (bufferedWriter != null) {
                bufferedWriter.close();
            }
        }
    }

    private static String[] getRecords(String record) {
        return record.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    }

    private static String getStringValue(String localRecord) {
        return "\"" + localRecord.replace("\"", "") + "\"";
    }

    private static void writeRawSQL(List<String> records, String header) throws IOException {
        String filePath = PATH+"\\movie-sql-"+(records.size())+".sql";
        System.out.println("filePath = " + filePath);

        String sqlInsert="insert into movies ("+header+") values (";

        FileWriter writer = new FileWriter(filePath);
        BufferedWriter bufferedWriter = new BufferedWriter(writer);

        try
        {
            int index=0;
            for (String record: records) {

                String[] localRecords= getRecords(record);
                String value=sqlInsert;
                //System.out.println(record);
                for (int i = 0; i < localRecords.length; i++) {
                    value+= getStringValue(localRecords[i]);

                    if(i<localRecords.length-1)
                        value+=",";
                }
                value+=");";

                bufferedWriter.write(value+"\n");

                index++;
            }
        } finally {
            if (bufferedWriter != null) {
                bufferedWriter.close();
            }
        }
    }
}
