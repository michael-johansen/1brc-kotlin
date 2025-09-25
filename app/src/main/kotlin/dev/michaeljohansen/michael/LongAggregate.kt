package dev.michaeljohansen.michael

import kotlin.math.max
import kotlin.math.min

data class LongAggregate(
    val count: Int = 0,
    val sum: Long = 0,
    val min: Long = Long.MAX_VALUE,
    val max: Long = Long.MIN_VALUE,
) {
    operator fun plus(other: LongAggregate): LongAggregate = LongAggregate(
        count + other.count,
        sum + other.sum,
        min(min, other.min),
        max(max, other.max),
    )

    operator fun plus(other: Long): LongAggregate = LongAggregate(
        count = count + 1,
        sum = sum + other,
        min = min(min, other),
        max = max(max, other),
    )

    fun toDescription(): String {
        val average = (1.0 * sum / count / 1_000_000_000)
        val min = (1.0 * min / 1_000_000_000)
        val max = (1.0 * max / 1_000_000_000)
        return String.format("%.3fs [%.3f, %.3f], %d", average, min, max, count)
    }
}