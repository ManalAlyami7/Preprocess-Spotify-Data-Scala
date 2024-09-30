import com.opencsv.CSVReader
import java.io.{File, FileReader, PrintWriter}
import scala.jdk.CollectionConverters.*
import scala.collection.mutable

object phase1 {
  def main(args: Array[String]): Unit = {
    val csvFilePath = "SpotifyFeatures.csv"
    val (cleanedData, missingRows, invalidRows, duplicateRows, outlierRows) = cleanData(csvFilePath)

    // Handle cleaned data
    saveData("cleaned_tracks.csv", cleanedData)

    // Handle missing rows
    saveExcludedRows("missing_rows.csv", missingRows, "Missing Rows")

    // Handle invalid rows
    saveExcludedRows("invalid_rows.csv", invalidRows, "Invalid Rows")

    // Handle duplicate rows
    saveExcludedRows("duplicate_rows.csv", duplicateRows, "Duplicate Rows")

    // Handle outlier rows
    saveExcludedRows("outlier_rows.csv", outlierRows, "Outlier Rows")

    // Handle reduced data if cleanedData is not empty
    if (cleanedData.nonEmpty) {
      val reducedData = reduceData(cleanedData)
      saveData("reduced_tracks.csv", reducedData)
    }
  }


  private def cleanData(csvFilePath: String): (List[String], List[String], List[String], List[String], List[String]) = {
    val reader = new CSVReader(new FileReader(csvFilePath))
    val lines = reader.readAll().asScala.toList

    if (lines.isEmpty) {
      println("The CSV file is empty.")
      return (List.empty[String], List.empty[String], List.empty[String], List.empty[String], List.empty[String])
    }

    // Read and clean the header
    val header = lines.head.map(_.trim.stripPrefix("\uFEFF")) // Remove BOM if present

    val trackIdColumnName = "track_id"
    val trackNameColumnName = "track_name"
    val artistNameColumnName = "artist_name"
    val genreColumnName = "genre" // Define the genre column name

    // Dynamically find the index of track_id, track_name, artist_name, and genre
    val trackIdIndex = header.indexWhere(_.equalsIgnoreCase(trackIdColumnName))
    val trackNameIndex = header.indexWhere(_.equalsIgnoreCase(trackNameColumnName))
    val artistNameIndex = header.indexWhere(_.equalsIgnoreCase(artistNameColumnName))
    val genreIndex = header.indexWhere(_.equalsIgnoreCase(genreColumnName)) // Add this line

    // Check for required columns
    if (trackIdIndex == -1 || trackNameIndex == -1 || artistNameIndex == -1 || genreIndex == -1) {
      println(s"Error: Column(s) '$trackIdColumnName', '$trackNameColumnName', '$artistNameColumnName' or '$genreColumnName' not found in the CSV header.")
      return (List.empty[String], List.empty[String], List.empty[String], List.empty[String], List.empty[String])
    }

    // Specify numerical columns for outlier detection
    val numericalColumns = List("popularity", "acousticness", "danceability", "duration_ms", "energy", "instrumentalness", "liveness", "loudness", "speechiness", "tempo", "valence").map(_.toLowerCase)
    val numericalColumnIndices = numericalColumns.flatMap(col => {
      val index = header.indexWhere(_.equalsIgnoreCase(col))
      if (index == -1) None else Some(index)
    })

    println(s"Starting cleaning process with ${lines.tail.size} rows...\n-------------------------------------------")

    val seenTrackIds = mutable.Set[String]()
    val seenTrackNames = mutable.Set[String]()
    var duplicateRows = List.empty[String]
    var missingRows = List.empty[String]
    var invalidRows = List.empty[String]
    var outlierRows = List.empty[String]
    val seenTrackGenres = mutable.Map[String, String]()

    // Collect numerical values for outlier detection
    val numericalValues = Array.fill(numericalColumns.size)(List.empty[Double])

    // Process each row
    val validRows = lines.tail.foldLeft(List.empty[String]) { (acc, line) =>
      val row = line.map(_.trim)
      val rowString = row.mkString(",")

      // Replace commas in track and artist names
      row(trackNameIndex) = row(trackNameIndex).replaceAll(",", " -")
      row(artistNameIndex) = row(artistNameIndex).replaceAll(",", " -")

      // Check for missing fields
      if (row.exists(_.isEmpty)) {
        val missingColumns = row.indices.filter(i => row(i).isEmpty).map(header(_)).mkString(", ")
        missingRows ::= s"Missing fields in line: $rowString. Missing columns: $missingColumns"
        acc
      }
      // Check for duplicates based on name and genre
      else if (seenTrackIds.contains(row(trackIdIndex))) {
        // Allow duplicates for different genres based on track_id
        if (seenTrackGenres.getOrElse(row(trackIdIndex), "") != row(genreIndex)) {
          // Valid duplicate due to genre, allow this entry
          acc // Keep the entry
        } else {
          duplicateRows ::= s"Duplicate track_id: ${row(trackIdIndex)} in line: $rowString"
          acc // Skip adding the duplicate row to the valid rows
        }
      }


      else {
        // Validate the row (using the validateColumns function)
        val errorReasons = validateColumns(row)
        if (errorReasons.nonEmpty) {
          invalidRows ::= s"$rowString. Reasons: ${errorReasons.mkString("; ")}"
          acc
        } else {
          // Mark track_id and track_name as seen
          seenTrackIds += row(trackIdIndex)
          seenTrackNames += row(trackNameIndex) // Also mark track_name as seen for future checks
          seenTrackGenres(row(trackNameIndex)) = row(genreIndex) // Store the genre for the track name
          val validRow = row.mkString(",") // Create a valid row string
          numericalColumnIndices.zipWithIndex.foreach { case (colIndex, numIndex) =>
            if (colIndex < row.length && row(colIndex).nonEmpty) {
              numericalValues(numIndex) ::= row(colIndex).toDouble // Collect numerical values for outlier detection
            }
          }
          validRow :: acc // Add valid row to the list
        }
      }
    }

    // Calculate outliers for each numerical column
    numericalColumnIndices.zipWithIndex.foreach { case (colIndex, numIndex) =>
      if (numericalValues(numIndex).nonEmpty) {
        val values = numericalValues(numIndex).sorted
        val q1 = values((values.size * 0.25).toInt)
        val q3 = values((values.size * 0.75).toInt)
        val iqr = q3 - q1

        val lowerBound = q1 - 3 * iqr
        val upperBound = q3 + 3 * iqr

        // Detect outliers
        lines.tail.foreach { line =>
          val row = line.map(_.trim)
          if (row(colIndex).nonEmpty) {
            val value = row(colIndex).toDouble
            if (value < lowerBound || value > upperBound) {
              outlierRows ::= s"Outlier in column '${header(colIndex)}' with value $value in line: ${row.mkString(",")} lowerBound :'$lowerBound' upperBound : '$upperBound'"
            }
          }
        }
      }
    }

    println(s"Cleaning completed. Valid rows: ${validRows.size}")
    println(s"There were ${missingRows.size} rows with missing fields.")
    println(s"There were ${invalidRows.size} rows with invalid fields.")
    println(s"There were ${duplicateRows.size} rows with duplicate track_id or track_name.")
    println(s"There were ${outlierRows.size} outliers detected.")

    reader.close()
    (header.mkString(",") :: validRows, missingRows, invalidRows, duplicateRows, outlierRows)
  }


  private def validateColumns(cols: Array[String]): List[String] = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    def isValidFloat(value: String): Boolean = {
      value.trim.matches("[-+]?\\d*\\.?\\d+([eE][-+]?\\d+)?")
    }

    def isNumeric(value: String): Boolean = {
      value.trim.matches("\\d+")
    }

    // Check for the correct number of columns
    if (cols.length != 18) {

      errors += s"Expected 18 columns, but found ${cols.length}."
      return errors.toList
    }

    // Validation checks
    if (isNumeric(cols(0))) {
      errors += "Genre cannot be a number."
    }
    // Artist name can be a number like band name
    // Track name can be a number
    if (isNumeric(cols(3))) {
      errors += "Track ID cannot be a number."
    }
    if (!cols(4).matches("\\d{1,3}") || cols(4).toInt < 0 || cols(4).toInt > 100) {
      errors += "Popularity must be a non-negative integer between 0 and 100."
    }
    if (!isValidFloat(cols(5)) || cols(5).toDouble < 0 || cols(5).toDouble > 1) {
      errors += "Acousticness must be a float between 0 and 1."
    }
    if (!isValidFloat(cols(6)) || cols(6).toDouble < 0 || cols(6).toDouble > 1) {
      errors += "Danceability must be a float between 0 and 1."
    }
    if (!cols(7).matches("\\d+")) {
      errors += "Duration_ms must be a non-negative integer."
    }
    if (!isValidFloat(cols(8)) || cols(8).toDouble < 0 || cols(8).toDouble > 1) {
      errors += "Energy must be a float between 0 and 1."
    }
    if (!isValidFloat(cols(9)) || cols(9).toDouble < 0 || cols(9).toDouble > 1) {
      errors += "Instrumentalness must be a float between 0 and 1."
    }
    if (!List("A", "A#", "B", "C", "C#", "D", "D#", "E", "F", "F#", "G", "G#").contains(cols(10).trim)) {
      errors += "Key must be a valid musical key (A, A#, B, etc.)."
    }
    if (!isValidFloat(cols(11)) || cols(11).toDouble < 0 || cols(11).toDouble > 1) {
      errors += "Liveness must be a float."
    }
    if (!isValidFloat(cols(12))) {
      errors += "Loudness must be a valid float value."
    }
    if (!List("Major", "Minor").contains(cols(13).trim)) {
      errors += "Mode must be either 'Major' or 'Minor'."
    }
    if (!isValidFloat(cols(14)) || cols(14).toDouble < 0 || cols(14).toDouble > 1) {
      errors += "Speechiness must be a float."
    }
    if (!isValidFloat(cols(15))) {
      errors += "Tempo must be a valid float value."
    }
    // Assuming cols(16) is in the format "X/Y"
    val timeSignature = cols(16)
    val pattern = "^([1-9]\\d*)/([1-9]\\d*)$".r // Regex to match positive integers

    if (!timeSignature.matches(pattern.regex)) {
      errors += "Time_signature must be in the format 'X/Y' where X and Y are positive integers not zeros."
    }
    if (!isValidFloat(cols(17)) || cols(17).toDouble < 0 || cols(17).toDouble > 1) {
      errors += "Valence must be a float between 0 and 1."
    }

    errors.toList
  }

  private def reduceData(cleanedData: List[String]): List[String] = {
    // Extract the original header from the cleaned data and handle potential BOM characters
    val originalHeader = cleanedData.head.split(",").map(_.trim.replace("\uFEFF", "").toLowerCase).toList
    val desiredColumns = List("genre", "artist_name", "track_name", "popularity", "acousticness", "danceability", "energy", "tempo").map(_.toLowerCase)

    // Find the indices of the desired columns and check for any missing columns
    val selectedColumnsIndices = desiredColumns.flatMap { col =>
      val index = originalHeader.indexOf(col)
      if (index == -1) {
        println(s"Warning: Column '$col' not found in the header.")
        None
      } else {
        Some(index)
      }
    }

    if (selectedColumnsIndices.isEmpty) {
      println("No valid columns found for reduction.")
      return List.empty[String] // Return empty if no valid columns found
    }

    println(s"Starting reduction process with ${cleanedData.tail.size} rows...\n-------------------------------------------")

    // Process all data
    val reducedRows = cleanedData.tail.map { line =>
      val cols = line.split(",").map(_.trim)
      if (selectedColumnsIndices.forall(i => i < cols.length)) { // Ensure the indices are valid
        selectedColumnsIndices.map(i => cols(i)).mkString(",")
      } else {
        "" // Handle cases where the row does not have enough columns
      }
    }.filter(_.nonEmpty) // Filter out any empty rows
    println(s"Reduction completed.")

    println(s"Number of rows after reduction: ${reducedRows.size}")
    println(s"Number of attributes after reduction: ${selectedColumnsIndices.size}")

    // Return header with reduced rows
    desiredColumns.mkString(",") :: reducedRows
  }

  private def saveData(filePath: String, data: List[String]): Unit = {
    val file = new File(filePath)
    if (data.nonEmpty) {
      val writer = new PrintWriter(file)
      writer.println(data.head) // Write header
      data.tail.foreach(writer.println) // Write each valid line
      writer.close()
      println(s"Data saved to $filePath.\n-------------------------------------------")
    } else {
      // Delete the file if it exists
      if (file.exists()) {
        file.delete()
       // println(s"$filePath has been deleted because it was empty.")
      }
    }
  }
  private def saveExcludedRows(filePath: String, data: List[String], category: String): Unit = {
    if (data.nonEmpty) {
      val writer = new PrintWriter(filePath)
      writer.println(s"$category:")
      data.foreach(writer.println)
      writer.close()
      println(s"$category saved to $filePath.\n-------------------------------------------")
    }
    else {
      // Delete the file if it exists
        val file = new File(filePath)
        if (file.exists()) {
        file.delete()
        println(s"$filePath has been deleted because it was empty.")
       }
    }
  }
  

}
