import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import scala.io.{BufferedSource, Source}
import java.util.logging.{FileHandler, Level, Logger}

val fileHandler = new FileHandler("C:Users/Omar/IdeaProjects/Rule_engine/src/main/rules_engine.log")
val logger: Logger = Logger.getLogger("Discount")
logger.addHandler(fileHandler)

val source: BufferedSource = Source.fromFile("C:Users/Omar/IdeaProjects/Rule_engine/src/main/TRX1000.csv")
val lines: List[String] = source.getLines().drop(1).toList

val f: File = new File("C:/Users/Omar/IdeaProjects/Rule_engine/src/main/processed_transactionss.csv")
val writer: PrintWriter = new PrintWriter(new FileOutputStream(f,false))

case class Transaction(timestamp: LocalDateTime, productName: String,
                       expiryDate: LocalDate, quantity: Int ,
                       unitPrice: Float, channel: String , paymentMethod : String)
def parseTimestamp(timestamp: String): LocalDateTime = {
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
  LocalDateTime.parse(timestamp, formatter)
}
def parseDate(dateString: String): LocalDate = {
  val formatter = DateTimeFormatter.ofPattern("M/d/yyyy")
  LocalDate.parse(dateString, formatter)
}
def toTransaction(line: String): Transaction = {
  val Array(timestamp, productName, expiryDate, quantityStr,
  unitPriceStr, channel, paymentMethod) = line.split(",")
  val parsedTimestamp = parseTimestamp(timestamp)
  val parsedExpiryDate = parseDate(expiryDate)
  val quantity = quantityStr.toInt
  val unitPrice = unitPriceStr.toFloat
  logger.info("transaction is created")
  Transaction(parsedTimestamp, productName, parsedExpiryDate,
    quantity, unitPrice, channel, paymentMethod)
}
def islessThan30DaysRemaining(from: LocalDateTime, to: LocalDate): Boolean = {
  val currentDate = LocalDate.from(from)
  val remainingDays = java.time.temporal.ChronoUnit.DAYS.between(currentDate, to)
  remainingDays < 30 && remainingDays >= 0
}
def isSpecialDate(date: LocalDateTime): Boolean = {
  date.getMonthValue == 3 && date.getDayOfMonth == 23
}
def isCheese(productName: String): Boolean = {
  productName.toLowerCase.contains("cheese")
}
def isWine(productName: String): Boolean = {
  productName.toLowerCase.contains("wine")
}
def isQuantityEligible(quantity: Int): Boolean = {
  quantity > 5
}

def isVisa(paymentMethod: String): Boolean = {
  paymentMethod.toLowerCase == "visa"
}
def isApp(channel: String): Boolean = {
  channel.toLowerCase == "app"
}

def calculateDiscount(transaction: Transaction) : Float = {
  val ExpiryDiscount = if (islessThan30DaysRemaining(transaction.timestamp , transaction.expiryDate)) {
    val daysRemaining = java.time.temporal.ChronoUnit.DAYS.between(transaction.timestamp.toLocalDate(), transaction.expiryDate).toInt
      30 - daysRemaining
  } else 0

  val specialDiscount = if (isSpecialDate(transaction.timestamp)) 50 else 0
  val quantityDiscount = if (isQuantityEligible(transaction.quantity)) {
    transaction.quantity match {
      case q if q >= 6 && q <= 9 => 5
      case q if q >= 10 && q <= 14 => 7
      case q if q > 15 => 10
      case _ => 0
    }
  } else 0
    val typeCheeseDiscount = if (isCheese(transaction.productName) ) 10 else 0
    val typeWineDiscount = if (isWine(transaction.productName) ) 5 else 0
  val VisaDiscount = if (isVisa(transaction.paymentMethod) ) 5 else 0
  val AppDiscount = if (isApp(transaction.channel)) {
    val reminder = transaction.quantity % 5
   if (reminder == 0) transaction.quantity
   else transaction.quantity + ( 5 - reminder)
  } else 0

  val largestTwoDiscounts = List(ExpiryDiscount, specialDiscount, quantityDiscount, typeCheeseDiscount, typeWineDiscount , VisaDiscount , AppDiscount).sortWith(_>_).take(2)

  val finalDiscount = (largestTwoDiscounts.sum.toFloat / 2 ) /100
  logger.info("discount is calculated")
 finalDiscount
  }
def processTransaction(t : Transaction): String = {
  val discount = calculateDiscount(t)
  logger.info("transaction is processed")
  s"${t.timestamp},${t.productName},${t.expiryDate},${t.quantity}," +
    s"${t.unitPrice},${t.channel},${t.paymentMethod} ,${discount}"
}

def writeLine(line: String): Unit = {
  writer.write(line + "\n")
}

writeLine("timestamp, productName, expiryDate, quantityStr," +
  "unitPriceStr, channel, paymentMethod , discount")
lines.map(toTransaction).map(processTransaction).foreach(writeLine)
writer.close()
