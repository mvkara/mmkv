module MKKV.CommonUtils
open MKKV.Storage
open System
open MMKV.Serialisers

let inline getInt64AtLocation (openedFile: IOpenedFile) (position: int64<LocationPointer>) = 
    let bytes = ArraySegment<_>(Array.zeroCreate<byte> sizeof<int64>)
    openedFile.ReadArray position bytes
    BitConverter.ToInt64(bytes.Array, bytes.Offset)

let inline writeInt64ToLocation (openedFile: IOpenedFile) (position: int64<LocationPointer>) (valueToWrite: int64) =
    let bytes = ArraySegment<_>(BitConverter.GetBytes(valueToWrite))
    openedFile.WriteArray bytes position

let inline locationPointer (v: int64) = v * 1L<LocationPointer>

let inline locationPointerFromInt (v: int) = (int64 v) * 1L<LocationPointer>

let inline deserialiseFromFile (serialiser: ISerialiser<_>) (openedFile: IOpenedFile) serialisationBuffer indexOffset  = 
    openedFile.ReadArray indexOffset serialisationBuffer
    serialiser.Deserialise serialisationBuffer

let inline serialiseToFile (serialiser: ISerialiser<_>) (openedFile: IOpenedFile) indexOffset o = 
    let data = serialiser.Serialise o
    openedFile.WriteArray data indexOffset

let inline subBuffer offset count (buf: ArraySegment<_>) = ArraySegment<_>(buf.Array, int offset, int count)