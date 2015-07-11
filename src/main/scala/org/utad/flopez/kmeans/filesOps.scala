package org.utad.flopez.kmeans

import java.util.Calendar
import java.io.{ FileWriter, BufferedWriter, File }

/**
 * @author flopez
 */
object filesOps {
  def printToFile(pathName: String, fileName: String, contents: String) = {
    val file = new File(pathName + "/" + fileName + ".txt")
    if (!file.exists()) {
      file.createNewFile()
    }

    val bw = new BufferedWriter(new FileWriter(file, true))
    bw.write(contents)
    bw.newLine()
    bw.close()
  }
}