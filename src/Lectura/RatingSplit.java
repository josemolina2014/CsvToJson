package Lectura;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RatingSplit {

    public static String PATH="C:\\Users\\josemolina\\Downloads\\ml-25m\\ml-25m";

    public static void main(String [] args) throws IOException {
        String fileNameToRead = PATH+"\\ratings.csv";
        List<Integer> samplesSizes = Arrays.asList(10,100,1000,10000,100000,1000000,10000000, 25000095);
        long total = getTotal(fileNameToRead);

        if(!samplesSizes.isEmpty())
        {
            for (Integer size : samplesSizes)
                readAndExportFile(fileNameToRead, size, total);
        }
        else
            readAndExportFile(fileNameToRead, -1, total);



    }

    private static void readAndExportFile(String fileNameToRead, int size, long total) throws IOException {

        FileReader fileReader = new FileReader(fileNameToRead);
        int fileSize=(size!=-1)?size:(int)total;

        String[] headers = null;
        String jsonFile = PATH+"\\rating-json-"+ fileSize +".json";
        FileWriter writerJson = new FileWriter(jsonFile);
        BufferedWriter bufferedWriterJson = new BufferedWriter(writerJson);

        String sqlFile = PATH+"\\rating-sql-"+ fileSize +".sql";
        BufferedWriter bufferedWriterSQL = new BufferedWriter(new FileWriter(sqlFile));


        try (BufferedReader bufferedReader = new BufferedReader(fileReader))
        {
            String line;
            String header="";
            int lineas=0;

            int lastLine=fileSize;
            if(size==-1)
                lastLine= (int)total-1;

            while((line = bufferedReader.readLine()) != null)
            {
                boolean isLastLine= lineas == lastLine;
                if(lineas==0)
                {
                    header = line;
                    headers = header.split(",");
                }
                else
                {
                    try
                    {
                        bufferedWriterJson.write(convertToJson(line, headers)+ (!isLastLine?",\n":""));
                        bufferedWriterSQL.write(covertToSQLInsert(line,header));
                    }
                    catch (Exception e){ }

                    if(size!=-1 && lineas== fileSize)
                        break;

                }
                lineas++;
            }
            System.out.println("lineas = " + lineas);
            bufferedWriterJson.close();
            bufferedWriterSQL.close();

        }
    }

    private static long getTotal(String fileNameToRead) throws IOException {
        FileReader fileReader = new FileReader(fileNameToRead);
        long total=0L;
        try (BufferedReader bufferedReader = new BufferedReader(fileReader))
        {
            total = bufferedReader.lines().count();
        }
        return total;
    }


    private static String convertToJson(String record, String[] headers) throws IOException {
        StringBuilder line= null;
        String[] localRecords= record.split(",");
        line= new StringBuilder();
        line.append("{");

        for (int i = 0; i < headers.length; i++) {
            line.append( "\"").append(headers[i]).append("\": ").append(localRecords[i]);

            if(i<headers.length-1)
                line.append(",");
        }
        line.append("}");

        return line.toString();
    }

    private static String covertToSQLInsert(String record, String header) throws IOException
    {
        StringBuilder line= new StringBuilder();;
        String sqlInsert="insert into rating ("+header+") values (";
        line.append(sqlInsert);

        String[] localRecords= record.split(",");
        for (int i = 0; i < localRecords.length; i++) {
            line.append(localRecords[i]) ;
            if(i<localRecords.length-1)
                line.append(",");
        }
        line.append(");\n");
        return line.toString();
    }
}
