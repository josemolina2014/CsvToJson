package Lectura;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Rating {

    public static String PATH="C:\\Users\\josemolina\\Downloads\\ml-25m\\ml-25m";

    public static void main(String [] args) throws IOException {
        String fileNameToRead = PATH+"\\ratings.csv";
        FileReader fileReader = new FileReader(fileNameToRead);
        List<Integer> samplesSizes = Arrays.asList(10000000);
        List<String> records = new ArrayList<>();
        boolean exportFull=true;

        try (BufferedReader bufferedReader = new BufferedReader(fileReader))
        {
            String header="";
            String line;
            int lineas=0;
            while((line = bufferedReader.readLine()) != null)
            {
                if(lineas==0)
                    header=line;
                else
                    records.add(line);

                if(!exportFull)
                {
                    for (Integer valueSize : samplesSizes)
                    {
                        if (valueSize.equals(lineas))
                        {
                            writeRawSQL(records, header);
                            writeRawJson(records,header);
                        }
                    }
                }

                lineas++;
            }
            System.out.println("lineas = " + lineas);

            if(exportFull)
            {
                System.out.println("Full Export");
                writeRawSQL(records, header);
             //   writeRawJson(records,header);
            }
        }
    }
    private static void writeRawSQL(List<String> records, String header) throws IOException {
        String filePath = PATH+"\\sql-rating-"+(records.size())+".sql";
        System.out.println("writeRawSQL");
        String sqlInsert="insert into rating ("+header+") values (";
        FileWriter writer = new FileWriter(filePath);
        try (
             BufferedWriter bufferedWriter = new BufferedWriter(writer)
             )
        {
            for (String record: records)
            {
                String[] localRecords= record.split(",");
                StringBuilder line= new StringBuilder();
                line.append(sqlInsert);
                for (int i = 0; i < localRecords.length; i++) {
                    line.append(localRecords[i]) ;

                    if(i<localRecords.length-1)
                        line.append(",");
                }
                line.append(");\n");

                bufferedWriter.write(line.toString());

            }
        }
        System.out.println("filePath = " + filePath +" Done");
    }

    private static void writeRawJson(List<String> records, String header) throws IOException {
        String filePath = "C:\\Users\\josemolina\\Downloads\\ml-25m\\ml-25m\\json-rating-"+(records.size())+".json";
        String[] headers= header.split(",");

        FileWriter writer = new FileWriter(filePath);
        try (
                BufferedWriter bufferedWriter = new BufferedWriter(writer)
            )
        {
            int index=0;
            for (String record: records) {

                String[] localRecords= record.split(",");
                StringBuilder line= new StringBuilder();
                line.append("{");

                for (int i = 0; i < headers.length; i++) {
                    line.append( "\"").append(headers[i]).append("\": ").append(localRecords[i]);

                    if(i<headers.length-1)
                        line.append(",");
                }
                line.append("}");
                if(index<records.size()-1)
                    line.append(",");

                bufferedWriter.write(line+"\n");

                index++;
            }
        }
        System.out.println("filePath = " + filePath +" Done");
    }
}
