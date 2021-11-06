package Lectura;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ReadAndCreateDocument {

    private String FOLDER_PATH="C:\\Users\\josemolina\\Downloads\\ml-25m\\ml-25m\\";
    private String moviesLocation = FOLDER_PATH+"movies.csv";
    private String ratingFileLocaton = FOLDER_PATH+"ratings.csv";

    private Map<String, List<String>> ratingsMap;

    public static void main(String[] args) throws IOException {
        /*tamaño o numero de lineas a exportar, si se quiere exportar
            el archivo completo dejar samplesSize=-1;
         */
        int samplesSize =-1;
        new ReadAndCreateDocument().loadFiles(samplesSize);
    }

    public void loadFiles(int samplesSize) throws IOException
    {
        boolean fullExport= samplesSize==-1;

        String moviesDocumentFileJson = null;
        if(fullExport)
            moviesDocumentFileJson = FOLDER_PATH+"document-movies-exp.json";
        else
            moviesDocumentFileJson = FOLDER_PATH+"document-movies-exp"+samplesSize+"-1.json";
        FileReader moviesFileReader = new FileReader(moviesLocation);
        FileWriter writerJson = new FileWriter(moviesDocumentFileJson);

        BufferedWriter bufferedWriterJson = new BufferedWriter(writerJson);

        String[] headers = null;
        ratingsMap = readAndLoadRating();


        int numRegistros=1;
        try (BufferedReader moviesReader = new BufferedReader(moviesFileReader))
        {
            String line;
            int lineas=0;

            int numBloques=2;

            while((line = moviesReader.readLine()) != null) {
                // boolean isLastLine= lineas == lastLine;
                if(lineas==0)
                {
                    headers = line.split(",");
                }
                else{

                    bufferedWriterJson.write(convertMovieToJson(line, headers));
                    if(!fullExport)
                    {
                        if(numRegistros==samplesSize){
                            bufferedWriterJson.close();

                            moviesDocumentFileJson = FOLDER_PATH+"document-movies-exp"+samplesSize+"-"+numBloques+".json";
                            System.out.println("moviesDocumentFileJson = " +moviesDocumentFileJson);
                            bufferedWriterJson = new BufferedWriter(new FileWriter(moviesDocumentFileJson));
                            numBloques++;
                            numRegistros=0;
                        }
                    }
                    numRegistros++;
                }

                lineas++;
            }

            if(numRegistros>1)
            {
                bufferedWriterJson.close();
                System.out.println("moviesDocumentFileJson = " + moviesDocumentFileJson);
            }
            System.out.println("lineas = " + (lineas-1));
        }
    }

    private Map<String, List<String>> readAndLoadRating() throws IOException {
        // El key value es el movieId
        Map<String, List<String>> ratings = new LinkedHashMap<>();

        StringBuilder movieRating=null;
        String line;
        int lineas=0;

        String preFix=",";
        String[] headers = null;
        FileReader fileReader = new FileReader(ratingFileLocaton);
        try (BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            while ((line = bufferedReader.readLine()) != null) {
                movieRating = new StringBuilder();
                if (lineas == 0) {
                    headers = line.split(",");
                } else {
                    String[] localRecords= line.split(",");

                    movieRating.append(ratingConvertToJson(localRecords, headers));

                    if(ratings.get(localRecords[1])!=null)
                    {
                        ratings.get(localRecords[1]).add(preFix+movieRating.toString());
                    }
                    else{
                        List<String> value = new ArrayList<>();
                        value.add(movieRating.toString());

                        ratings.put(localRecords[1], value);
                    }
                }
                lineas ++;
            }
        }

        return ratings;
    }
    private String convertMovieToJson(String record, String[] headers) throws IOException {
        StringBuilder line= new StringBuilder();
        String[] localRecords= splitLineByComma(record);
        headers[0]="_id"; // se ajusta al formato de id para couchDB
        line.append("{");
        String movieYear= "";
        for (int i = 0; i < headers.length; i++) {
            line.append("\"").append(headers[i] ).append("\": ");
            if(i==2)
                line.append("[").append(getGenresValue(localRecords[i])).append("]");
            else
                line.append(getStringValue(localRecords[i]));

            //if (i < headers.length - 1)
            line.append(",");

            if(i==1)
                movieYear= extractYear(localRecords[i]);
        }
        line.append("\"year\":").append(movieYear).append(",");
        line.append(findRatingByMovieId(localRecords[0]));
        line.append("}\n");
        return line.toString();
    }

    private String findRatingByMovieId(String movieId) throws IOException
    {
        StringBuilder movieRating = new StringBuilder();
        movieRating.append("\"ratings\": [ ");

        List<String> values = ratingsMap.get(movieId);

        if(values!=null)
        {
            for (String ratingValue : values) {
                movieRating.append(ratingValue);
            }
        }
        movieRating.append("]");

        return movieRating.toString();
    }

    /**
     * Transforma la linea a un formato json
     * @param localRecords
     * @param headers
     * @return
     * @throws IOException
     */
    private static String ratingConvertToJson(String[] localRecords, String[] headers) throws IOException
    {

        StringBuilder line= new StringBuilder();
        line.append("{");
        for (int i = 0; i < headers.length; i++) {
            if(i==1) continue; // se omite el movieId

            line.append( "\"").append(headers[i]).append("\": ").append(localRecords[i]);

            if(i<headers.length-1)
                line.append(",");
        }
        line.append("}");


        return line.toString();
    }

    /**
     * seapra los generos separados por | y lo expresa en forma de matriz json
     * @param localRecord
     * @return
     */
    private static String getGenresValue(String localRecord) {
        if(localRecord.equalsIgnoreCase("(no genres listed)")) return "";
        String[] generes = localRecord.split("\\|");
        return Arrays.stream(generes)
                .map(plain -> '"' + (plain) + '"')
                .collect(Collectors.joining(","));
    }

    /**
     * obtiene el valor del campo com comillas dobles
     * @param localRecord
     * @return
     */
    private static String getStringValue(String localRecord) {
        return "\"" + localRecord.replace("\"", "") .replaceAll("'","")+ "\"";
    }

    /**
     * se encarga de hacer la division de la cadena usando la comma como separador
     * en este caso si la cadena tiene una comilla doble no la divide,
     * ejemplo la linea 11,"American President, The (1995)",Comedy|Drama|Romance
     * quedaria asi,<br>
     * 0-> 11 <br>
     * 1-> "American President, The (1995)" <br>
     * 2-> Comedy|Drama|Romance
     * @param line linea de evaluacion
     * @return lista de los elementos separados por coma
     */
    private static String[] splitLineByComma(String line) {
        return line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    }

    /**
     * Extrae el año del titulo de la pelicula
     * @param text titulo de la pelicula
     * @return año de la pelicula o 0 sino lo encuentra
     */
    private static String extractYear(String text)
    {
        String regex ="(\\d{4})";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(text);
        if (matcher.find()) {
            return matcher.group();
        }
        else
            return "0";
    }

}
