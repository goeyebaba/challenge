import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The Normalizer class is responsible for normalizing CSV data according to rules
 * defined in (https://github.com/trussworks/truss-interview/blob/main/CSV_README.md)
 * It processes input file, applies transformations, and generates output file.
 * It could be ran on command line as "Normalizer &lt;input-file&gt; &lt;output-file&gt;"
 */
public class Normalizer {
	public static final int MAX_ERROR_COUNT = 100;
	public static final String NON_UNICODE_CHAR = "[^\\u0000-\\uFFFF]";
	public static final String UNICODE_REPLACEMENT_CHAR = "\uFFFD";
	public static final String ZERO_STR = "0";

	/**
	 * Enum representing various fields in the CSV data.
	 * All constants are ordered in the default order of the fields in input file.
	 * A corresponding transformation function could be retrieved by the field.
	 */
	public enum Field {
        TIMESTAMP,
        ZIP,
        FULLNAME,
        ADDRESS,
        FOODURATION,
        BARDURATION,
        TOTALDURATION,
        NOTES;

		public TransformationStrategy getTransformationStrategy() {
            switch (this) {
                case TIMESTAMP:
                    return Transformer::normalizeTimestamp;
                case ZIP:
                    return Transformer::normalizeZip;
                case FULLNAME:
                    return Transformer::normalizeFullName;
                case ADDRESS:
                    return Transformer::normalizeAddress;
                case FOODURATION:
                    return Transformer::normalizeFooDuration;
                case BARDURATION:
                    return Transformer::normalizeBarDuration;
                case NOTES:
                    return Transformer::normalizeNotes;
                default:
                    throw new UnsupportedOperationException("Unsupported field: " + this);
            }
        }
    }

	/**
	 * The main method handles files I/O and delegate record processing to Processor.
	 * Here are the steps of process
	 * 1. taking 2 parameters: &lt;input_file&gt; -- name of the input file
	 *                         &lt;output_file&gt; -- name of the output file
	 * 2. Open input file for read; open output file for write
	 * 3. Instantiate a Processor
	 * 4. Processor read each line from input file and process
	 * 5. Write processed line into output file
	 */
	public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println("Usage: java Normalizer <input_file> <output_file>");
            return;
        }

        String inputFile = args[0];
        String outputFile = args[1];

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), StandardCharsets.UTF_8));
             PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8))) {

            String line;
            String normalizedLine;
            int lineNbr = 1;
            Processor csvProcessor = new Processor(MAX_ERROR_COUNT);

            while ((line = br.readLine()) != null && !line.isEmpty()) {
                String[] data = csvProcessor.parseCSV(lineNbr, line);
                normalizedLine = data != null? csvProcessor.processData(lineNbr, data) : null;
                if(normalizedLine != null)
                	pw.println(normalizedLine);

                lineNbr++;
            }
            System.out.println(outputFile + " has been generated");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	/**
	 * This class is responsible for parsing CSV data, applying transformations and handling errors.
	 * It also provide a feature to handle input in different orders:
	 * If the input file has a header, it will process data according to the header.
	 * If the input file does not have a header, it will process data according to the default order defined in the Enum Field.
	 */
	public static class Processor {

		private final ErrorTracker errorTracker;

		//this property object is used to store the input fields order: the key is the field name, the value is the position.
		//If the input file has a header, the order will be based on the header; otherwise it will use the default order defined in the Enum Field.
		private Map<Field, Integer> fieldIndexMap = new EnumMap<>(Field.class);

		/**
		 * Processor constructor
		 *
		 * @param maxErrorCount This parameter is used to instantiate an ErrorTracker object. Processor delegate
		 *                      the error tracking task to ErrorTracker
		 */
		public Processor(int maxErrorCount) {
	        errorTracker = new ErrorTracker(maxErrorCount);
	    }

		/**
		 * parse an input line
		 *
		 * @param lineNbr the purpose of this parameter is for reporting error at specific location
		 * @param line the input data, a line of the input file
		 * @return a string array, the input line is broken down into fields
		 */
		public String[] parseCSV(int lineNbr, String line) {

			//split the line by commas, also consider the double quotation situation
	        String[] result = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

	        //if the line is not split into 8 parts, then consider it not properly formed
	        if (result != null && result.length!= 8) {

	        	//dealing with the exception where the last field is empty
	        	if(result.length == 7 && line.trim().endsWith(","))
	        	{
	        		String[] newResult = Arrays.copyOf(result, 8);
	        		newResult[7] = ""; // Set the 8th element to an empty string
	        		return newResult;
	        	}

	            handleException(lineNbr, new Exception("Invalid CSV format"));
	            return new String[0];
	        }

	        return result;
	    }

		/**
		 * process the data in one input line
		 *
		 * @param lineNbr the purpose of this parameter is for reporting error at specific location
		 * @param data a string array containing data for each field
		 * @return a string with processed data, all fields are concatenated back with commas
		 */
		public String processData(int lineNbr, String[] data)
		{

			String normalizedLine = null;

			//for the first input line, check if it is header
			if(lineNbr == 1 && isTitleLine(data, fieldIndexMap))
	    	{
	    		normalizedLine = String.join(",", data);
	    		return normalizedLine;
	    	}
	    	else  //if it's not header or not first line, delegate the transformation/normalization job to Transformer
	    	{
	    		try {
	        		Transformer.transform(fieldIndexMap, data);
	        		normalizedLine = String.join(",", data);
	        		return normalizedLine;
	    		}catch(Exception ex) {
	    			handleException(lineNbr, ex);
	    		}
	    	}

	        return normalizedLine;
		}

		/**
		 * Determine if the input line is the header, and also populate fieldIndexMap
		 * If it is not the header, populate fieldIndexMap based on the default order defined in the Enum Field
		 *
		 * @param data the input string array with all the fields
		 * @param fieldIndexMap a EnumMap that will be populated with the field order information
		 * @return a boolean indicating whether the input line is the header
		 */
	    private boolean isTitleLine(String[] data, Map<Field, Integer> fieldIndexMap) {

	    	boolean result = checkTitleLine(data, fieldIndexMap);

	    	if(!result)
	    	{
	    		fieldIndexMap.clear();

	            int position = 0; // Starting position

	            for (Field field : Field.values()) {
	            	fieldIndexMap.put(field, position++);
	            }

	    	}
			return result;
		}

	    /**
		 * Determine if the input line is the header, and also populate fieldIndexMap
		 * If it is the header, populate fieldIndexMap based on the header
		 *
		 * @param data the input string array with all the fields
		 * @param fieldIndexMap a EnumMap that will be populated with the field order information
		 * @return a boolean indicating whether the input line is the header
		 */
	    private boolean checkTitleLine(String[] data, Map<Field, Integer> fieldIndexMap) {

	    	Set<String> header = new HashSet<>();
	    	for (Field field : Field.values()) {
	            header.add(field.name());
	        }

	    	int i = 0;
	    	for(String element: data)
	    	{
	    		element = element.trim().toUpperCase();
	    		if(header.contains(element)){
	    			fieldIndexMap.put(Enum.valueOf(Field.class, element), i++);
	    		}
	    		else
	    			return false;

	    	}

	    	return i == 8? true : false;
		}

	    /**
		 * Encapsulate the use of ErrorTracker for error handling
		 *
		 * @param lineNbr to report error with position information
		 * @param ex the exception generated
		 */
		private void handleException(int lineNbr, Exception ex) {
            errorTracker.incrementErrorCount();

            if (errorTracker.shouldStopProcessing()) {
                System.err.println("Max error count reached. Stopping processing.");
                System.exit(1); // Exit the application or handle it as needed
            }

            System.err.println("Error on line #" + lineNbr);
            System.err.println(ex.getMessage());
        }
	}

	/**
	 * This class is responsible for normalizing each field of the input line
	 * It uses the Enum Field's getTransformationStrategy() to retrieve the transformation functions for each field and execute them.
	 * The transformation functions will be executed based on the default order defined in Enum Field.
	 */
    public static class Transformer {

    	/**
		 * Private constructor to prevent instantiation
		 */
        private Transformer() {
            // Private constructor to prevent instantiation
            // This constructor is intentionally left empty
        }

    	/**
		 * Normalize all the fields in an input line
		 *
		 * @param index EnumMap indicates positions of all fields.
		 * @param data string array contains all the fields in their input order
		 */
    	public static void transform(Map<Field, Integer> index, String[] data)
    	{
    		for (Field field : Field.values()) {
    			int i = index.get(field);

    			//dealing with the exception of field TotalDuratihttps://marketplace.eclipse.org/marketplace-client-intro?mpc_install=2568658on, this field doesn't have its corresponding transformationStrategy mapping.
    			//Since this field has dependencies on two other fields: FooDuration and BarDuration, the transform function has to be called
    			// at the line level but not individual field level.
    			if(field == Field.TOTALDURATION)
                {
                	String fooDuration = data[index.get(Field.FOODURATION)];
                	String barDuration = data[index.get(Field.BARDURATION)];
                	data[i] = normalizeTotalDuration(fooDuration, barDuration);
                }
    			else
    			{
	    			TransformationStrategy transformationStrategy = field.getTransformationStrategy();
	    			//replace invalid UTF-8 encoding with replacement encoding then
	    			//normalize the text to ensures that combining characters are properly combined with their base characters.
	    			data[i] = data[i].replaceAll(NON_UNICODE_CHAR, UNICODE_REPLACEMENT_CHAR);
	    			data[i] = java.text.Normalizer.normalize(data[i], java.text.Normalizer.Form.NFC);

	    			//apply the corresponding transformation logic
	                data[i] = transformationStrategy.transform(data[i]);
    			}
            }
    	}

    	/**
		 * Transformation function for Timestamp field, follows the TransformationStrategy functional interface
		 *
		 * @param timestamp  Timestamp field of the input line
		 * @return a normalized string for Timestamp field
		 */
    	public static String normalizeTimestamp(String timestamp)
    	{
    		try {
	    		DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("M/d/yy h:mm:ss a");
		        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX")
		                .withZone(ZoneId.of("US/Eastern"));
		        ZonedDateTime zonedDateTime = ZonedDateTime.parse(timestamp, inputFormatter
		                .withZone(ZoneId.of("US/Pacific")));
		        return outputFormatter.format(zonedDateTime);
    		}catch (DateTimeParseException ex) {
    			throw new DateTimeParseException("Error parsing Timestamp: " + ex.getMessage(), ex.getParsedString(), ex.getErrorIndex(), ex);
    	    }
    	}

    	/**
		 * Transformation function for Zip field, follows the TransformationStrategy functional interface
		 *
		 * @param zip  Zip field of the input line
		 * @return a normalized string for zip field
		 */
    	public static String normalizeZip(String zip) {
    		try {
    			if(zip.trim().isEmpty())
    				return zip;
    			else if(zip.length() > 5)
    				throw new ZipLengthException(" '" + zip + "' is longer than 5");
    			else
    				return String.format("%05d", Integer.parseInt(zip));
    		}catch(Exception ex) {
    			throw new ZipFormatException("Error parsing Zip: " + ex.getMessage());
    		}

    	}

    	/**
		 * Transformation function for FullName field, follows the TransformationStrategy functional interface
		 *
		 * @param fullName  FullName field of the input line
		 * @return a normalized string for fullName field
		 */
    	public static String normalizeFullName(String fullName) {
    		return fullName.toUpperCase();
    	}

    	/**
		 * Transformation function for Address field, follows the TransformationStrategy functional interface
		 * It's currently not doing much, placeholder for potential transformation logics
		 *
		 * @param address  Address field of the input line
		 * @return a normalized string for Address field
		 */
    	public static String normalizeAddress(String address) {
    		return address;
    	}

    	/**
		 * Transformation function for FooDuration field, follows the TransformationStrategy functional interface
		 *
		 * @param fooDuration  FooDuration field of the input line
		 * @return a normalized string for FooDuration field
		 */
    	public static String normalizeFooDuration(String fooDuration) {
    		try {
    			 if(fooDuration.trim().isEmpty())
    				 return ZERO_STR;

	    		 return convertTimeToSeconds(fooDuration);
    		}catch(NumberFormatException ex)
    		{
    			throw new DurationFormatException("Error parsing FooDuration: " + ex.getMessage(), ex);
    		}
    	}

    	/**
		 * Transformation function for BarDuration field, follows the TransformationStrategy functional interface
		 *
		 * @param barDuration  BarDuration field of the input line
		 * @return a normalized string for BarDuration field
		 */
    	public static String normalizeBarDuration(String barDuration) {
    		try {
    			if(barDuration.trim().isEmpty())
   				 return ZERO_STR;

	    		return convertTimeToSeconds(barDuration);
    		}catch(NumberFormatException ex)
    		{
    			throw new DurationFormatException("Error parsing BarDuration: " + ex.getMessage(), ex);
    		}
    	}

    	/**
		 * Transformation function for TotalDuration field
		 *
		 * @param fooDuration  FooDuration field of the input line
		 * @param barDuration  BarDuration field of the input line
		 * @return a normalized string for TotalDuration field
		 */
    	public static String normalizeTotalDuration(String fooDuration, String barDuration) {
    		try {
	    		long foo = Long.parseLong(fooDuration);
	    		long bar = Long.parseLong(barDuration);
	    		return String.valueOf(foo + bar);
    		}catch(NumberFormatException ex)
    		{
    			throw new DurationFormatException("Error parsing BarDuration: " + ex.getMessage(), ex);
    		}
    	}

    	/**
		 * Transformation function for Notes field, follows the TransformationStrategy functional interface
		 * It's currently not doing much, placeholder for potential transformation logics
		 *
		 * @param notes  Notes field of the input line
		 * @return a normalized string for Notes field
		 */
    	public static String normalizeNotes(String notes) {
    		return notes;
    	}

    	/**
		 * Transformation a time string into a string in seconds
		 *
		 * @param time  input time string
		 * @return a string in seconds
		 */
    	private static String convertTimeToSeconds(String time) {
    		LocalTime localTime;
    		try {
    			localTime = LocalTime.parse(time, DateTimeFormatter.ofPattern("H:mm:ss.SSS"));
    		}catch(DateTimeParseException ex)
    		{
    			localTime = LocalTime.parse(time, DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));
    		}
	        return String.valueOf(localTime.toSecondOfDay());
	    }

    }

    /**
	 * This class is responsible for tracking error count
	 * And it signals whether the error count reaches predefined maximum number.
	 */
    public static class ErrorTracker {
        private final int maxErrorCount;
        private int errorCount;

        /**
		 * Constructor
		 *
		 * @param maxErrorCount  The error count triggers termination of current process
		 */
        public ErrorTracker(int maxErrorCount) {
            this.maxErrorCount = maxErrorCount;
            this.errorCount = 0;
        }

        /**
		 * Increase the error count by 1.
		 */
        public synchronized void incrementErrorCount() {
            errorCount++;
        }

        /**
		 * check if the error count reaches the predefined max error count number.
		 * @return a boolean to indicate whether the current process should be stopped
		 */
        public synchronized boolean shouldStopProcessing() {
            return errorCount >= maxErrorCount;
        }
    }

    /**
	 * Functional Interface mapped to the field transformation functions
	 */
    @FunctionalInterface
    public interface TransformationStrategy {
        String transform(String input);
    }

    public static class TimestampParseException extends RuntimeException {
		private static final long serialVersionUID = 4147739777611795160L;

		public TimestampParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class ZipFormatException extends RuntimeException {
		private static final long serialVersionUID = 5779911776777707790L;

		public ZipFormatException(String message) {
            super(message);
        }
    }

    public static class ZipLengthException extends RuntimeException {
		private static final long serialVersionUID = -7466080156350975480L;

		public ZipLengthException(String message) {
            super(message);
        }
    }

    public static class DurationFormatException extends RuntimeException {
		private static final long serialVersionUID = 8048001799788440418L;

		public DurationFormatException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
