package com.eztier

import java.util.Date
import java.text.SimpleDateFormat
import java.io.{File, FileInputStream, FileWriter, InputStream, PrintWriter}

import scala.collection.JavaConversions.asScalaIterator

import org.apache.commons.io.{FilenameUtils, IOUtils}
import org.apache.tika.exception.TikaException
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.{AutoDetectParser, ParseContext, Parser}
import org.apache.tika.parser.audio.AudioParser
import org.apache.tika.parser.html.HtmlParser
import org.apache.tika.parser.image.ImageParser
import org.apache.tika.parser.microsoft.OfficeParser
import org.apache.tika.parser.opendocument.OpenOfficeParser
import org.apache.tika.parser.pdf.PDFParser
import org.apache.tika.parser.rtf.RTFParser
import org.apache.tika.parser.txt.TXTParser
import org.apache.tika.parser.xml.XMLParser
import org.apache.tika.sax.WriteOutContentHandler

sealed trait FileType { 
  def getParser: Parser  
}
class Text extends FileType {
  override def getParser = new TXTParser()
}
class Html extends FileType {
  override def getParser = new HtmlParser()
}
class Xml extends FileType {
  override def getParser = new XMLParser()
}
class Pdf extends FileType {
  override def getParser = new PDFParser()
}
class Rtf extends FileType {
  override def getParser = new RTFParser()
}
class OOText extends FileType {
  override def getParser = new OpenOfficeParser()
}
class MsExcel extends FileType {
  override def getParser = new OfficeParser()
}
class MsWord extends FileType {
  override def getParser = new OfficeParser()
}
class MsPowerpoint extends FileType {
  override def getParser = new OfficeParser()
}
class MsOutlook extends FileType {
  override def getParser = new OfficeParser()
}
class Visio extends FileType {
  override def getParser = new OfficeParser()
}
class Png extends FileType {
  override def getParser = new ImageParser()
}
class Jpeg extends FileType {
  override def getParser = new ImageParser()
}
class Mp3 extends FileType {
  override def getParser = new AudioParser()
}

case class Extracted(
  author: String,
  title: String,
  date: String,
  body: String
)

/*
  This class is based on below with some modifications.
  https://github.com/sujitpal/mia-scala-examples/blob/master/src/main/scala/com/mycompany/mia/preprocess/TextExtractor.scala
*/
class TextExtractor {
  private val dateFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'"

  private val parsers = Map[String, FileType](
    "text" -> new Text(),
    "html" -> new Html(),
    "htm" -> new Html(),
    "xml" -> new Xml(),
    "pdf" -> new Pdf(),
    "rtf" -> new Rtf(),
    "odt" -> new OOText(),
    "xls" -> new MsExcel(),
    "xlsx" -> new MsExcel(),
    "doc" -> new MsWord(),
    "docx" -> new MsWord(),
    "ppt" -> new MsPowerpoint(),
    "pptx" -> new MsPowerpoint(),
    "pst" -> new MsOutlook(),
    "vsd" -> new Visio(),
    "png" -> new Png(),
    "jpg" -> new Jpeg(),
    "jpeg" -> new Jpeg(),
    "mp3" -> new Mp3()
  ) 

  def detectFileType(file: File): Option[FileType] = {
    val suffix = FilenameUtils.getExtension(file.getName()).toLowerCase()
    if (parsers.contains(suffix)) Some(parsers(suffix)) else None
  }

  def extract(file: File): Option[Extracted] = {
    var istream: InputStream = null
    try {
      istream = new FileInputStream(file)
      val handler = new WriteOutContentHandler(-1)
      val metadata = new Metadata()
      val maybeParser = detectFileType(file)

      maybeParser match {
        case Some(parser) =>
          val ctx = new ParseContext()
          parser.getParser.parse(istream, handler, metadata, ctx)
          
          Some(
            Extracted(
              metadata.get(Metadata.CREATOR),
              metadata.get(Metadata.TITLE),
              metadata.get(Metadata.DATE),
              handler.toString()
            )
          )
        case _ => None
      }
    } catch {
      case e: TikaException => 
        println(e.getMessage())
        None      
    } finally {
      IOUtils.closeQuietly(istream)
    }
  }
}

object TextExtractor {
  def apply = new TextExtractor
}