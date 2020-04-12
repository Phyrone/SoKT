package de.phyrone.sokt

import com.fasterxml.jackson.dataformat.protobuf.ProtobufMapper
import com.fasterxml.jackson.dataformat.protobuf.schema.ProtobufSchema
import io.ktor.network.sockets.ServerSocket
import io.ktor.network.sockets.Socket
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.Closeable
import java.io.IOException
import kotlin.reflect.KClass

class SoKT(
    socket: Socket,
    private val mapper: ProtobufMapper,
    private val registeredPackets: Set<RegisteredPacket>
) :
    Closeable by socket {

    private val packetHeadSchema = mapper.generateSchemaFor(PacketHead::class.java)
    private val inChannel = socket.openReadChannel()
    private val outChannel = socket.openWriteChannel(false)
    private val readPacketLock = Mutex()
    private val sendLock = Mutex()
    suspend fun sendPacket(packet: Any) = sendLock.withLock {
        val registeredPacket = registeredPackets
            .find { registeredPacket -> registeredPacket.clazz == packet::class.java }
            ?: throw IllegalArgumentException("packet not registered")
        val packetBodyBytes = mapper.writer(registeredPacket.schema).writeValueAsBytes(packet)
        val headerPacket = PacketHead(registeredPacket.id, packetBodyBytes.size)
        val headerBytes = mapper.writer(packetHeadSchema).writeValueAsBytes(headerPacket)
        val headerSize = headerBytes.size.toByte()
        outChannel.writeByte(headerSize)
        outChannel.writeByteArray(headerBytes)
        outChannel.writeByteArray(packetBodyBytes)
        outChannel.flush()
    }

    private suspend fun ByteWriteChannel.writeByteArray(bytes: ByteArray) {
        bytes.forEach { byte -> writeByte(byte) }
    }

    suspend fun readPacket() = readPacketLock.withLock {
        val headerSize = inChannel.readByte().toInt()
        val headerPacketBytes = inChannel.readByteArray(headerSize)
        val headerPacket = mapper.readerFor(PacketHead::class.java)
            .with(packetHeadSchema)
            .readValue<PacketHead>(headerPacketBytes)
        val bytes = inChannel.readByteArray(headerPacket.size)
        readPacketBody(headerPacket.packetId, bytes)
    }

    private fun readPacketBody(id: Int, bytes: ByteArray): Any {
        val registeredPacket =
            registeredPackets.find { registeredPacket -> registeredPacket.id == id } ?: throw UnknownPacketException()
        return mapper.readerFor(registeredPacket.clazz).with(registeredPacket.schema).readValue(bytes)
    }


    private suspend fun ByteReadChannel.readByteArray(size: Int): ByteArray {
        val result = ByteArray(size)
        repeat(size) { result[it] = readByte() }
        return result
    }


    companion object Static {
        fun builder() = SoKTBuilder()
    }

}

class ServerSoKT(
    private val serverSocket: ServerSocket,
    private val mapper: ProtobufMapper,
    private val registerdPackets: Set<RegisteredPacket>
) : Closeable by serverSocket {

    suspend fun accept() = SoKT(serverSocket.accept(), mapper, registerdPackets)

    companion object Static {
        fun builder() = SoKTBuilder()
    }
}

data class RegisteredPacket(val id: Int, val clazz: Class<*>, val schema: ProtobufSchema)

class SoKTBuilder {
    private val modifiers = HashSet<ProtobufMapper.() -> Unit>()
    fun withJacksonModifier(modifier: ProtobufMapper.() -> Unit): SoKTBuilder {
        modifiers.add(modifier)
        return this
    }

    private val packetsToRegister = HashSet<Pair<Int, Class<*>>>()
    fun withPackets(vararg packets: Pair<Int, Class<*>>): SoKTBuilder {
        packetsToRegister.addAll(packets)
        return this
    }

    private fun buildRegisteredPackets(mapper: ProtobufMapper) = packetsToRegister.map { pair ->
        RegisteredPacket(
            pair.first,
            pair.second,
            mapper.generateSchemaFor(pair.second)
        )
    }.toSet()

    fun buildClient(socket: Socket): SoKT {
        val mapper = ProtobufMapper()
        modifiers.forEach { modifier -> modifier.invoke(mapper) }
        val registeredPackets = buildRegisteredPackets(mapper)
        return SoKT(socket, mapper, registeredPackets)
    }

    fun buildServer(socket: ServerSocket): ServerSoKT {
        val mapper = ProtobufMapper()
        modifiers.forEach { modifier -> modifier.invoke(mapper) }
        val registeredPackets = buildRegisteredPackets(mapper)
        return ServerSoKT(socket, mapper, registeredPackets)
    }
}

fun SoKTBuilder.withPackets(vararg packets: Pair<Int, KClass<*>>): SoKTBuilder =
    withPackets(*packets.map { pair -> Pair(pair.first, pair.second.java) }.toTypedArray())

private data class PacketHead(
    val packetId: Int,
    val size: Int
)

class UnknownPacketException : IOException()