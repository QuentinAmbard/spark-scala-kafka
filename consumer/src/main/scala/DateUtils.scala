package com.datastax.demo.fraudprevention

import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit

/**
  * Created by quentin on 29/01/17.
  */
object DateUtils {
  private val SEC_MIN = 60
  private val SEC_HOUR = 60 * 60
  private val SEC_DAY = 60 * 60 * 24
  private val SEC_HALF_DAY = 60 * 60 * 12
  private val SEC_WEEK = 60 * 60 * 24 * 7
  //1th janury 1970 was a thursday. Need to remove 3 days to start the week to monday.
  private val WEEK_OFFSET = 60 * 60 * 24 * 4

  /**
    * Round the given timestamp to the midnight value of the period.
    *
    * @param timestamp the date to round
    * @param unit      any ChronoUnit.
    * @return the rounded timestamp. If Day, timestamp of the same day at midnight. If Month, timestamp of the 01 of the month at midnight etc.
    */
  def roundTimestamp(timestamp: Int, unit: ChronoUnit): Int = {
    unit match {
      case ChronoUnit.SECONDS =>   timestamp
      case ChronoUnit.MINUTES =>   timestamp / SEC_MIN * SEC_MIN
      case ChronoUnit.HOURS =>   timestamp / SEC_HOUR * SEC_HOUR
      case ChronoUnit.HALF_DAYS =>    timestamp / SEC_HALF_DAY * SEC_HALF_DAY
      case ChronoUnit.DAYS =>   timestamp / SEC_DAY * SEC_DAY
      case ChronoUnit.WEEKS =>   (timestamp - WEEK_OFFSET) / SEC_WEEK * SEC_WEEK + WEEK_OFFSET
      case u if u == ChronoUnit.MONTHS || u == ChronoUnit.YEARS => {
          val instant = Instant.ofEpochSecond(timestamp)
          val datetime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC)
          val year = datetime.getYear
          var month = 1
          if (unit eq ChronoUnit.MONTHS) {
            month = datetime.getMonthValue
          }
          val dateYear = LocalDateTime.of(year, month, 1, 0, 0, 0, 0)
          return dateYear.toEpochSecond(ZoneOffset.UTC).toInt
        }
      case _ => throw new IllegalArgumentException("unsupported unit" + unit)
    }
  }

  def roundTimestamp(datetime: LocalDateTime , unit: ChronoUnit): LocalDateTime = {
    if (unit == ChronoUnit.MONTHS || unit == ChronoUnit.YEARS || unit == ChronoUnit.DAYS) {
      val year = datetime.getYear
      var month = 1
      var day = 1
      if (unit == ChronoUnit.MONTHS || unit == ChronoUnit.DAYS) {
        month = datetime.getMonthValue
        if (unit == ChronoUnit.DAYS) {
          day = datetime.getDayOfMonth
        }
      }
      LocalDateTime.of(year, month, day, 0, 0, 0, 0)
    } else {
      throw new IllegalArgumentException("unsupported unit" + unit)
    }
  }

}