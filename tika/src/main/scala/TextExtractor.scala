package com.eztier

import java.io.{File, FileInputStream, FileWriter, InputStream, PrintWriter}

import scala.collection.JavaConversions.asScalaIterator

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

class TextExtractor {

}

object TextExtractor {
  def apply = new TextExtractor
}