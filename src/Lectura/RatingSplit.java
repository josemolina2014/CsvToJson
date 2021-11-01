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

    /**
     *
     * @param fileNameToRead nombre del archivo a leer
     * @param sampleSize tamaño de la muestra a exportar, -1 para indicar que no hay
     *                   restriccion y se exporta el archivo completo
     * @param totalLines tamaño total del archivo
     * @throws IOException
     */
    private static void readAndExportFile(String fileNameToRead, int sampleSize, long totalLines) throws IOException {

        FileReader fileReader = new FileReader(fileNameToRead);
        int exportFileSize=(sampleSize!=-1)?sampleSize:(int)totalLines;

        String[] headers = null;
        String jsonFile = PATH+"\\rating-json-"+ exportFileSize +".json";
        FileWriter writerJson = new FileWriter(jsonFile);
        BufferedWriter bufferedWriterJson = new BufferedWriter(writerJson);

        String sqlFile = PATH+"\\rating-sql-"+ exportFileSize +".sql";
        BufferedWriter bufferedWriterSQL = new BufferedWriter(new FileWriter(sqlFile));


        try (BufferedReader bufferedReader = new BufferedReader(fileReader))
        {
            String line;
            String header="";
            int lineas=0;

            int lastLine=exportFileSize;
            if(sampleSize==-1)
                lastLine= (int)totalLines-1;

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

                    if(sampleSize!=-1 && lineas== exportFileSize)
                        break;

                }
                lineas++;
            }
            System.out.println("lineas = " + lineas);
            bufferedWriterJson.close();
            bufferedWriterSQL.close();

        }
    }

    /**
     * obtiene el total de registros del archivo csv
     * @param fileNameToRead
     * @return
     * @throws IOException
     */
    private static long getTotal(String fileNameToRead) throws IOException {
        FileReader fileReader = new FileReader(fileNameToRead);
        long total=0L;
        try (BufferedReader bufferedReader = new BufferedReader(fileReader))
        {
            total = bufferedReader.lines().count();
        }
        return total;
    }

    /**
     * Transforma la linea a un formato json
     * @param record
     * @param headers
     * @return
     * @throws IOException
     */
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

    /**
     * Transforma la linea en una instruccion insert sql
     * @param record
     * @param header
     * @return
     * @throws IOException
     */
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
