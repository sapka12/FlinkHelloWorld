package batch

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

case class Trip(
                 pickUpLocation: String,
                 destination: String,
                 passangerCount: Int
               )

case class Cab(
                id: String,
                numberPlate: String,
                _type: String,
                driverName: String,
                ongoingTrip: Option[Trip]
              )

case class CountBy(byType: String, passangerSum: Double, tripCount: Int)

object CabFlink {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    val csv = env.readTextFile(params.get("input"))

    val cabs = csv.map{line =>
      val row = line.split(",")
      val trip = if (row(4) == "yes") Some(Trip(row(5), row(6), row(7).toInt)) else None
      Cab(row(0), row(1), row(2), row(3), trip)
    }

    // Task #1: Popular destination
    val popularDestination = cabs
      .flatMap(_.ongoingTrip.map(trip => (trip.destination, trip.passangerCount)))
      .groupBy(_._1)
      .reduce((tripA, tripB) => (tripA._1, tripA._2 + tripB._2))
      .maxBy(1)

    popularDestination.writeAsText(params.get("output-popular"))

    // Task #2: Average number of passengers from each pickup location
    val avgNumPassangers = cabs
      .flatMap(_.ongoingTrip)
      .map(trip => CountBy(trip.pickUpLocation, trip.passangerCount, 1))
      .groupBy(_.byType)
      .reduce{(t1, t2) => CountBy(
        t1.byType,
        t1.passangerSum + t2.passangerSum,
        t1.tripCount + t2.tripCount
      )}
      .map(tripCount => (tripCount.byType, tripCount.passangerSum / tripCount.tripCount))

    avgNumPassangers.writeAsText(params.get("output-avgpassangers"))


    // Task #3: Average number of trips for each driver
    val avgPassangerPerDriver = cabs
      .flatMap(cab => cab.ongoingTrip.map(trip => CountBy(cab.driverName, trip.passangerCount, 1)))
      .groupBy(_.byType)
      .reduce{(t1, t2) => CountBy(
        t1.byType,
        t1.passangerSum + t2.passangerSum,
        t1.tripCount + t2.tripCount
      )}
      .map(tripCount => (tripCount.byType, tripCount.passangerSum / tripCount.tripCount))

    avgPassangerPerDriver.writeAsText(params.get("output-avgpassangersperdriver"))

    env.execute("CabFlink")
  }
}
