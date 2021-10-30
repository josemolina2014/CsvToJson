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
        List<Integer> samplesSizes = Arrays.asList(10,100,1000,10000);
        List<String> records = new ArrayList<>();
        boolean exportFull=false;

        StringBuffer buffer = new StringBuffer();

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
            if(exportFull)
            {
                writeRawSQL(records, header);
                writeRawJson(records,header);
            }

            System.out.println("lineas = " + lineas);
        }
    }
    private static void writeRawSQL(List<String> records, String header) throws IOException {
        String filePath = PATH+"\\sql-rating-"+(records.size())+".sql";
        System.out.println("filePath = " + filePath);

        String sqlInsert="insert into rating ("+header+") values (";
        FileWriter writer = new FileWriter(filePath);
        BufferedWriter bufferedWriter = new BufferedWriter(writer);

        try
        {
            int index=0;
            for (String record: records) {

                String[] localRecords= record.split(",");
                String value=sqlInsert;
                //System.out.println(record);
                for (int i = 0; i < localRecords.length; i++) {
                    value+= localRecords[i] ;

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

    private static void writeRawJson(List<String> records, String header) throws IOException {
        String filePath = "C:\\Users\\josemolina\\Downloads\\ml-25m\\ml-25m\\json-"+(records.size())+".json";
        System.out.println("filePath = " + filePath);
        //File file = File.createTempFile(filePath, ".csv");

        String[] headers= header.split(",");



        FileWriter writer = new FileWriter(filePath);
        BufferedWriter bufferedWriter = new BufferedWriter(writer);

        try
        {
            int index=0;
            for (String record: records) {

                String[] localRecords= record.split(",");
                String value="{";
                //System.out.println(record);
                for (int i = 0; i < headers.length; i++) {
                    value+= "\""+headers[i]+"\": "+localRecords[i];

                    if(i<headers.length-1)
                        value+=",";
                }

                if(index<records.size()-1)
                    value+="},";
                else
                    value+="}";

                bufferedWriter.write(value+"\n");



                index++;
            }
            //writer.flush(); // close() should take care of this
            //bufferedWriter.close();
        } finally {
            if (bufferedWriter != null) {
                bufferedWriter.close();
            }
        }
    }
}
