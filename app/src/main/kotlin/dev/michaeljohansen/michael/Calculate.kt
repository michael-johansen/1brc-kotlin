package dev.michaeljohansen.michael

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import java.lang.System.getProperty
import java.lang.foreign.Arena.global
import java.lang.foreign.MemorySegment
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption

object Calculate {
    private const val FILE = "./measurements.txt"
    private const val CHUNK_SIZE = 1024 * 1024 * 10
    private val runtime = Runtime.getRuntime()

    init {
        println("Working Directory = ${getProperty("user.dir")}");
    }

    // Desktop: 30 sec
    // Laptop: 3 min
    @JvmStatic
    fun main(args: Array<String>) {
        FileChannel.open(Path.of(FILE), StandardOpenOption.READ).use { channel ->
            val processors = runtime.availableProcessors()
            println("Available processors: $processors")
            val fileSize = channel.size()
            val arena = global()
            val memorySegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, arena)
            val ranges = positionsAndSize(channel)
            check(ranges.sumOf { it.second } == fileSize) { "chunk sizes is not equal to file size" }

            val resultsFromThreads: Sequence<HashMap<String, DoubleAggregate>> = runBlocking(Dispatchers.Default) {
                val workers = ranges.map { (start, size) ->
                    val async = async {
                        calculateForChunk(memorySegment, start, size).also {
                            println("completed $start+$size")
                        }
                    }
                    async
                }
                // workers.awaitAll().asSequence()
                var notCompleted = workers
                val sequence = sequence {
                    while (notCompleted.isNotEmpty()) {
                        val (completed, stillActive) = notCompleted.partition { it.isCompleted }
                        println("completed: ${completed.size}, still active: ${stillActive.size}")
                        notCompleted = stillActive
                        for (deferred in completed) {
                            yield(runBlocking { deferred.await() })
                        }
                    }
                }
                sequence

            }

            // val workQueue = LinkedBlockingQueue<Runnable>()
            // val executor = ThreadPoolExecutor(processors, processors, 1, TimeUnit.SECONDS, workQueue)
            // val resultsFromThreads = synchronizedList(arrayListOf<Map<String, DoubleAggregate>>())
            // ranges.forEach { (start, size) ->
            //     executor.submit {
            //         val result = try {
            //             calculateForChunk(memorySegment, start, size)
            //         } catch (e: Exception) {
            //             e.printStackTrace()
            //             exitProcess(1)
            //         }
            //         Benchmark.benchmark("gather results from workers") {
            //             resultsFromThreads.add(result)
            //         }
            //         println("segment $start+$size complete")
            //     }
            // }
            // Benchmark.benchmark("await.threads") {
            //     executor.shutdown()
            //     while (executor.activeCount > 0) {
            //         println("active threads: ${executor.activeCount}, remaining: ${workQueue.size}")
            //         Thread.sleep(1000)
            //     }
            //     executor.awaitTermination(60, TimeUnit.SECONDS)
            // }

            val mergedResults = resultsFromThreads.fold<HashMap<String, DoubleAggregate>, Map<String, DoubleAggregate>>(emptyMap()) { old, new ->
                (old.keys + new.keys)
                    .associateWith { key ->
                        val oldValue = old[key]
                        val newValue = new[key]
                        val aggregate: DoubleAggregate = when {
                            oldValue != null && newValue != null -> oldValue + newValue
                            newValue != null -> newValue
                            oldValue != null -> oldValue
                            else -> error("")
                        }
                        aggregate
                    }
            }
            mergedResults.mapValues<String, DoubleAggregate, Result> { (k, v) -> Result(v.min, v.sum / v.count, v.max) }
                .entries
                .sortedBy<Map.Entry<String, Result>, String> { (key, _) -> key }
                .forEach<Map.Entry<String, Result>> { println("${it.key}: ${it.value}") }
            // println("resultsFromThreads.size: ${resultsFromThreads.count()}")
            // println("resultsFromThreads.size: ${resultsFromThreads.flatMap { it.values }.sumOf { it.count }}")
        }
    }

    private fun calculateForChunk(
        memorySegment: MemorySegment,
        start: Long,
        size: Long,
    ): HashMap<String, DoubleAggregate> {
        val rowsOfBytes = readChunk(memorySegment, start, size).readKeyValues().toList()
        val result = HashMap<String, DoubleAggregate>()
        for ((city, tempBytes) in rowsOfBytes) {
            val temperature = tempBytes.decodeToString().toDouble()
            val existing = result[city]
            if (existing == null) {
                result[city] = DoubleAggregate(1, temperature, temperature, temperature)
            }
            else {
                result[city] = existing + temperature
            }
        }
        return result
    }

    enum class ReadMode {
        CITY,
        TEMPERATURE
    }

    private fun ByteArray.readKeyValues(): Sequence<Pair<String, ByteArray>> = sequence {
        val lineBreak: Byte = 10
        val semiColon: Byte = 59
        val cityBuffer = ByteBuffer.allocate(32)
        val temperatureBuffer = ByteBuffer.allocate(8)
        var mode = ReadMode.CITY
        var char = 0
        var line = 0
        for (byte in this@readKeyValues) {
            try {
                when (byte) {
                    lineBreak -> {
                        mode = ReadMode.CITY
                        val cityBytes = cityBuffer.array().copyOf(cityBuffer.position())
                        val temperatureBytes = temperatureBuffer.array().copyOf(temperatureBuffer.position())
                        yield(cityBytes.decodeToString() to temperatureBytes)
                        // val city = String(cityBuffer.array(), 0, cityBuffer.position())
                        // val temperature = String(temperatureBuffer.array(), 0, temperatureBuffer.position())
                        // println("$city, $temperature")

                        cityBuffer.clear()
                        temperatureBuffer.clear()
                        line++
                    }

                    semiColon -> mode = ReadMode.TEMPERATURE
                    else -> when (mode) {
                        ReadMode.CITY -> cityBuffer.put(byte)
                        ReadMode.TEMPERATURE -> temperatureBuffer.put(byte)
                    }
                }
                char++
            } catch (e: Exception) {
                println(
                    """
                    |Failed on reading byte index $char on line $line which was ${Char(byte.toInt())}. Buffers:
                    | city: $cityBuffer (${cityBuffer.array().decodeToString()}),
                    | temperature: $temperatureBuffer (${temperatureBuffer.array().decodeToString()}),
                    | """.trimMargin()
                )
                throw e
            }
        }

    }

    private fun readChunk(memorySegment: MemorySegment, start: Long, size: Long): ByteArray {
        val buffer = ByteArray(size.toInt())
        val slice1 = memorySegment.asSlice(start, size)
        val byteBuffer1 = slice1.asByteBuffer()
        byteBuffer1.get(0, buffer)
        return buffer
    }

    private fun positionsAndSize(
        channel: FileChannel,
        delimiter: Byte = 10,
    ): List<Pair<Long, Long>> {
        val fileSize = channel.size()
        val startPositions = (0L..fileSize).step(CHUNK_SIZE.toLong())
        val buffer = ByteBuffer.allocate(256)
        val adjustedPositions = listOf(0L) + startPositions
            .drop(1) // don't adjust 0
            .map { position ->
                channel.position(position)
                channel.read(buffer)
                val indexOf = buffer.array().indexOf(delimiter)
                buffer.clear()
                // next section starts after delimiter
                position + indexOf + 1

            } + listOf(fileSize)
        return adjustedPositions.zipWithNext()
            .map { (start, end) ->
                start to (end - start)
            }
    }

    data class Result(
        val min: Double,
        val average: Double,
        val max: Double,
    )
}

