package Lectura;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class Movies {
    public static String PATH="C:\\Users\\josemolina\\Downloads\\ml-25m\\ml-25m";

    public static void main(String [] args) throws IOException {
        String fileName = PATH+"\\movies.csv";
        FileReader fileReader = new FileReader(fileName);
        List<Integer> samplesSizes = Arrays.asList(10,100,1000, 10000, 50000, 62423 );
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

                String[] localRecords = getRecords(record);
                StringBuilder line= new StringBuilder();
                line.append("{");

                for (int i = 0; i < headers.length; i++) {
                    line.append("\"").append(headers[i] ).append("\": ");
                    if(i==2)
                        line.append("[").append(getGenresValue(localRecords[i])).append("]");
                    else
                        line.append(getStringValue(localRecords[i]));

                    if (i < headers.length - 1)
                        line.append(",");
                }

                line.append("}");
                if (index < records.size() - 1)
                    line.append(",");

                bufferedWriter.write(line+"\n");

                index++;
            }
        }
    }

    private static String[] getRecords(String record) {
        return record.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    }

    private static String getStringValue(String localRecord) {
        return "\"" + localRecord.replace("\"", "") .replaceAll("'","")+ "\"";
    }

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



    private static void writeRawSQL(List<String> records, String header) throws IOException {
        String filePath = PATH+"\\movie-sql-"+(records.size())+".sql";
        System.out.println("filePath = " + filePath);

        String sqlInsert="insert into movies ("+header+") values (";
        FileWriter writer = new FileWriter(filePath);

        HashMap<String, Integer> genresMap = new LinkedHashMap<>();
        List<String> genresList = new ArrayList<>();

        try (BufferedWriter bufferedWriter = new BufferedWriter(writer))
        {
            int index=0;
            for (String record: records) {

                String[] localRecords= getRecords(record);
                StringBuilder line= new StringBuilder();
                line.append(sqlInsert);
                String movieId= "";
                for (int i = 0; i < localRecords.length; i++)
                {
                    if(i==0)
                        movieId= localRecords[i];

                    if(i==2)
                    {
                        extractGenresValuesToSQL(genresMap, genresList, localRecords[i], movieId);
                    }
                    else
                        line.append(getStringValue(localRecords[i]));

                    if(i<localRecords.length-2)
                        line.append(",");
                }
                line.append(");\n");

                bufferedWriter.write(line.toString());

                index++;
            }
        }
        exportGenresValues(records.size(), genresMap);
        exportMovieGenresValues(genresList, records.size());
    }

    private static void exportGenresValues(int records, HashMap<String, Integer> genresMap) throws IOException {
        String filePath = PATH+"\\genres-sql-"+(records)+".sql";
        FileWriter writer = new FileWriter(filePath);
        try (BufferedWriter bufferedWriter = new BufferedWriter(writer))
        {
            for (Map.Entry<String, Integer> entry : genresMap.entrySet()) {
                bufferedWriter.write("insert into genres (id, name) values (" + entry.getValue()+",\""+entry.getKey() + "\");\n");
            }
        }
    }

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

    private static void extractGenresValuesToSQL(HashMap<String, Integer> genresMap, List<String> genresList, String localRecord, String movieId) {
        String[] genres =getGenresValueToSQL(localRecord);
        for (int val = 0; val < genres.length; val++) {
            if(!genres[val].isEmpty())
            {
                if(genresMap.get(genres[val])==null){
                    genresMap.put(genres[val],genresMap.size()+1);
                }
                genresList.add("insert into movie_genres (movieId, genreId) values ("+movieId+","+genresMap.get(genres[val])+");\n" );
            }
        }
    }
}
