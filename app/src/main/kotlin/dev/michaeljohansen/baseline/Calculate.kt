package dev.michaeljohansen.baseline

import java.nio.file.Files
import kotlin.io.path.Path
import kotlin.math.max
import kotlin.math.min
import kotlin.streams.asSequence
import kotlin.time.measureTime

object Calculate {
    const val FILE = "./measurements.txt"

    // ThinkPad: Total time: 306363 ms (~100% on 1 core, <150MB, underutilized IO)
    // Desktop: Total time: 75865 ms (~100% on 1 core, <150MB, underutilized IO)
    // Desktop: compared to CalculateAverage_thomaswue 1652 ms (best java solution)
    @JvmStatic
    fun main(args: Array<String>) {
        println("Starting")
        val duration = measureTime {
            val aggregates =
                Files.lines(Path(FILE))
                    .asSequence()
                    .map { line ->
                        val (city, temperature) = line.split(";")
                        city to temperature
                    }
                    .groupingByAggregate()
                    .entries
                    .sortedBy { (city, _) -> city }
            aggregates.forEach { (city, aggregate) -> println("$city: $aggregate") }
            check(aggregates.sumOf { it.value.count } == 1_000_000_000) { "not everything is counted" }
        }
        println("Total time: ${duration.inWholeMilliseconds} ms")
    }

    data class Aggregate(
        var min: Double = Double.POSITIVE_INFINITY,
        var max: Double = Double.NEGATIVE_INFINITY,
        var sum: Double = 0.0,
        var count: Int = 0,
    ) {
        override fun toString(): String = "$min/${String.format("%.1f", sum / count)}/$max"
    }

    // Slightly problematic since it will load everything inot memory
    fun Sequence<Pair<String, String>>.groupByAggregate(): Map<String, Aggregate> =
        groupBy({ (city, _) -> city }, { (_, temperature) -> temperature })
            .mapValues { (key, values) ->
                val temperatures = values.map { it.toDouble() }
                Aggregate(
                    min = temperatures.min(),
                    max = temperatures.max(),
                    sum = temperatures.sum(),
                    count = temperatures.count(),
                )
            }

    fun Sequence<Pair<String, String>>.groupingByAggregate(): Map<String, Aggregate> =
        groupingBy { (city, _) -> city }
            .fold(initialValue = Aggregate()) { accumulator, (_, temperatureString) ->
                val temperature = temperatureString.toDouble()
                accumulator.copy(
                    min = min(accumulator.min, temperature),
                    max = max(accumulator.max, temperature),
                    sum = accumulator.sum + temperature,
                    count = accumulator.count + 1,
                )
            }

}
