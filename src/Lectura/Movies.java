package Lectura;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Movies {
    public static String PATH="C:\\Users\\josemolina\\Downloads\\ml-25m\\ml-25m\\";

    public static void main(String [] args) throws IOException {
        String fileName = PATH+"movies.csv";
        FileReader fileReader = new FileReader(fileName);
        List<Integer> samplesSizes = Arrays.asList();
        List<String> records = new ArrayList<>();
        boolean exportFull=samplesSizes.isEmpty();

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

    /**
     * Genera un archivo Json a partir de la estrucutra del archivo csv,
     * @param records
     * @param header
     * @throws IOException
     */
    private static void writeRawJson(List<String> records, String header) throws IOException {
        String filePath = PATH+"\\movies-json-" + (records.size()) + ".json";
        System.out.println("filePath = " + filePath);

        String[] headers = header.split(",");
        headers[0]="_id";

        FileWriter writer = new FileWriter(filePath);


        try ( BufferedWriter bufferedWriter = new BufferedWriter(writer))
        {
            int index = 0;
            for (String record : records) {

                String[] localRecords = splitLineByComma(record);
                StringBuilder line= new StringBuilder();
                line.append("{");
                String movieYear= "";
                for (int i = 0; i < headers.length; i++) {
                    line.append("\"").append(headers[i]).append("\": ");
                    if(i==2)
                        line.append("[").append(getGenresValue(localRecords[i])).append("]");
                    else
                        line.append(getStringValue(localRecords[i]));

                    if (i < headers.length - 1)
                        line.append(",");

                    if(i==1)
                        movieYear= extractYear(localRecords[i]);
                }

                    line.append(",").append("\"year\":").append(movieYear);

                line.append("}");
                if (index < records.size() - 1)
                    line.append(",");

                bufferedWriter.write(line+"\n");

                index++;
            }
        }
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
     * obtiene el valor del campo com comillas dobles
     * @param localRecord
     * @return
     */
    private static String getStringValue(String localRecord) {
        return "\"" + localRecord.replace("\"", "") .replaceAll("'","")+ "\"";
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

    private static String [] getGenresValueToSQL(String localRecord) {
        if(localRecord.equalsIgnoreCase("(no genres listed)")) return new String[]{""};
        String[] generes = localRecord.split("\\|");
        return generes;
    }


    /**
     * Genera un archivo sql a partir de la estrucutra del archivo csv,
     * adicionalmente genera los insert de la tabla  generos y la tabla de relacion entre peliculas y generos
     * @param records registros a procesar
     * @param header encabeazado del archivo
     * @throws IOException
     */
    private static void writeRawSQL(List<String> records, String header) throws IOException {
        String filePath = PATH+"\\movie-sql-"+(records.size())+".sql";
        System.out.println("filePath = " + filePath);

        String sqlInsert="insert into movies ("+header+",year ) values (";
        FileWriter writer = new FileWriter(filePath);

        HashMap<String, Integer> genresMap = new LinkedHashMap<>();
        List<String> genresList = new ArrayList<>();

        try (BufferedWriter bufferedWriter = new BufferedWriter(writer))
        {
            int index=0;
            for (String record: records) {

                String[] localRecords= splitLineByComma(record);
                StringBuilder line= new StringBuilder();
                line.append(sqlInsert);
                String movieId= "";
                String movieYear= "";
                for (int i = 0; i < localRecords.length; i++)
                {
                    if(i==0)
                        movieId= localRecords[i];
                    if(i==1)
                        movieYear= extractYear(localRecords[i]);

                    if(i==2)
                    {
                        extractGenresValuesToSQL(genresMap, genresList, localRecords[i], movieId);
                    }
                    else
                        line.append(getStringValue(localRecords[i]));


                    if(i<localRecords.length-2)
                        line.append(",");
                }
                line.append(","+movieYear);
                line.append(");\n");

                bufferedWriter.write(line.toString());

                index++;
            }
        }
        exportGenresValues(records.size(), genresMap);
        exportMovieGenresValues(genresList, records.size());
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

    /**
     * exporta a un archivo de texto el comando insert para sql de la tabla generos a partir del HashMap con la lista
     * de generos encontrados dentro del archivo
     * @param records tamaño del archivo de peliculas exportado
     * @param genresMap HashMap con los generos unicos encontrados dentro del documento csv
     * @throws IOException
     */
    private static void exportGenresValues(int records, HashMap<String, Integer> genresMap) throws IOException {
        String filePath = PATH+"\\genres-sql-"+(records)+".sql";
        FileWriter writer = new FileWriter(filePath);
        try (BufferedWriter bufferedWriter = new BufferedWriter(writer))
        {
            for (Map.Entry<String, Integer> entry : genresMap.entrySet()) {
                bufferedWriter.write("insert into genres (id, genre) values (" + entry.getValue()+",\""+entry.getKey() + "\");\n");
            }
        }
    }

    /**
     * exporta a un archivo de texto las instrucciones insert para sql de la tabla de la relacion entre peliculas y generos que esta
     * alamacenado en la lista genresList que se recibe como parametro
     * @param genresList Lista con los insert para la tabla movie_genres
     * @param records tamaño del archivo de peliculas exportado
     * @throws IOException
     */
    private static void exportMovieGenresValues(List<String> genresList, int records) throws IOException {
        String filePath = PATH+"\\movie-genres-sql-"+(records)+".sql";
        FileWriter writer = new FileWriter(filePath);

        try (BufferedWriter bufferedWriter = new BufferedWriter(writer))
        {
            for (String value: genresList) {
                    bufferedWriter.write(value);
            }
        }
    }

    /**
     * seapra los generos separados por | y les agrega un id unico a cada registro y los alamacena en un HasMap
     * adicionalmente genera una lista de String List<String> con los insert para la tabla de la relacion entre peliculas
     * y generos llamada movie_generes
     *
     * @param genresMap
     * @param genresList
     * @param localRecord
     * @param movieId
     */
    private static void extractGenresValuesToSQL(HashMap<String, Integer> genresMap, List<String> genresList, String localRecord, String movieId) {
        String[] genres =getGenresValueToSQL(localRecord);
        for (int val = 0; val < genres.length; val++) {
            if(!genres[val].isEmpty())
            {
                if(genresMap.get(genres[val])==null){
                    genresMap.put(genres[val],genresMap.size()+1);
                }
                genresList.add("insert into MOVIEBYGENRE (movieid, genreid) values ("+movieId+","+genresMap.get(genres[val])+");\n" );
            }
        }
    }
}
