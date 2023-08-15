# CSV Normalization
This is an assignment for Truss.Work job interview (https://github.com/trussworks/truss-interview/blob/main/CSV_README.md)

## Run it as class files
\>javac Normalizer.java

\>java Normalizer \<input-file\> \<output-file\>

## Run it as a Jar file
Normalizer.java has a few nested classes, it will generate multiple class files after compilation. You might find it easier to have it as a Jar file, especially if you want to move the runnable code around. 

\>javac Normalizer.java

\>jar cfe Normalizer.jar Normalizer *.class

\>java -jar Normalizer.jar \<input-file\> \<output-file\>

## Notes
1. If the input file has header, it will process the data in the order of the header; if there's no header, it will assume the order is as such: Timestamp, Zip, FullName, Address, FooDuration, BarDuration, TotalDuration, Notes.
2. If it fails to process a line, it will move on to the next line.
3. If it fails to process a field, it will stop processing the rest of the line and move on to the next line.
4. If error count reaches the set maximum (by default it's set to 100), it will stop processing the file.
5. The source codes are commented in Java Doc style, you can generate Java Doc by running the following command

   \>javadoc -d \<output-directory> Normalizer.java
   
