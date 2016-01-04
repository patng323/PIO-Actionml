package io.prediction.core

import grizzled.slf4j.Logger
import io.prediction.annotation.DeveloperApi
import io.prediction.data.storage.Event
import io.prediction.data.store.{LEventStore, PEventStore}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/** :: DeveloperApi ::
  * Base class of cleaned data source.
  *
  * A cleaned data source consists tools for cleaning events that happened earlier that
  * specified duration in seconds from train moment. Also it can remove duplicates and compress
  * properties(flat set/unset events to one)
  *
  */
@DeveloperApi
trait CleanedDataSource {

  /** :: DeveloperApi ::
    * Current App name which events will be cleaned.
    *
    * @return App name
    */
  @DeveloperApi
  def appName: String

  /** :: DeveloperApi ::
    * Param list that used for cleanup.
    *
    * @return current event windows that will be used to clean up events.
    */
  @DeveloperApi
  def eventWindow: Option[EventWindow] = None

  @transient lazy val logger = Logger[this.type]

  /** :: DeveloperApi ::
    *
    * Returns RDD of events happend after duration in event window params.
    *
    * @return RDD[Event] most recent PEvents.
    */
  @DeveloperApi
  def getCleanedPEvents(sc: SparkContext): RDD[Event] = {
    val pEvents = PEventStore.find(appName)(sc)

    eventWindow
      .flatMap(_.duration)
      .map { duration =>
        pEvents.filter(e =>
          e.eventTime.isAfter(DateTime.now().minusMillis(duration))
        )
      }.getOrElse(pEvents)
  }

  /** :: DeveloperApi ::
    *
    * Returns Iterator of events happend after duration in event window params.
    *
    * @return Iterator[Event] most recent LEvents.
    */
  @DeveloperApi
  def getCleanedLEvents(): Iterator[Event] = {

    val lEvents = LEventStore.find(appName)

    eventWindow
      .flatMap(_.duration)
      .map { duration =>
        lEvents.filter(e =>
          e.eventTime.isAfter(DateTime.now().minusMillis(duration))
        )
      }.getOrElse(lEvents)
  }

  def compressPProperties(): Unit = {}

  def compressLProperties(): Unit = {}

  def removePDuplicates(): Unit = {}

  def removeLDuplicates(): Unit = {}

  /** :: DeveloperApi ::
    *
    * Filters most recent, compress properties and removes duplicates of PEvents
    *
    * @return RDD[Event] most recent PEvents
    */
  @DeveloperApi
  def cleanedPEvents(sc: SparkContext): RDD[Event] = {
    cleanPEvents()
    getCleanedPEvents(sc)
  }

  /** :: DeveloperApi ::
    *
    * Filters most recent, compress properties of PEvents
    */
  @DeveloperApi
  def cleanPEvents(): Unit = {
    eventWindow.map { ew =>
      if (ew.compressProperties) compressPProperties()
      if (ew.removeDuplicates) removePDuplicates()
    }
  }

  /** :: DeveloperApi ::
    *
    * Filters most recent, compress properties and removes duplicates of LEvents
    *
    * @return Iterator[Event] most recent LEvents
    */
  @DeveloperApi
  def cleanedLEvents: Iterator[Event] = {
    cleanLEvents()
    getCleanedLEvents()
  }

  /** :: DeveloperApi ::
    *
    * Filters most recent, compress properties of LEvents
    */
  @DeveloperApi
  def cleanLEvents(): Unit = {
    eventWindow.map { ew =>
      if (ew.compressProperties) compressLProperties()
      if (ew.removeDuplicates) removeLDuplicates()
    }
  }
}

case class EventWindow(
  duration: Option[Int] = None,
  removeDuplicates: Boolean = false,
  compressProperties: Boolean = false
)
