package dev.michaeljohansen.michael

import kotlin.math.max
import kotlin.math.min

data class DoubleAggregate(
    val count: Int = 0,
    val sum: Double = 0.0,
    val min: Double = Double.MAX_VALUE,
    val max: Double = Double.MIN_VALUE,
) {
    operator fun plus(other: DoubleAggregate): DoubleAggregate = DoubleAggregate(
        count + other.count,
        sum + other.sum,
        min(min, other.min),
        max(max, other.max),
    )

    operator fun plus(other: Double): DoubleAggregate = DoubleAggregate(
        count = count + 1,
        sum = sum + other,
        min = min(min, other),
        max = max(max, other),
    )
}