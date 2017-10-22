namespace MMKV

open System
open System.IO
open System.IO.MemoryMappedFiles
open System.Collections.Generic
open System.Runtime.InteropServices
open System.Runtime.Serialization.Formatters
open System.Runtime.Serialization.Formatters.Binary
open MMKV.Serialisers
open MKKV.Storage
open MKKV.CommonUtils

type [<Struct; CLIMutable>] internal ImmutableLookupIndex = {
    ValueLocationWithVariableLength: ValueLocationWithVariableLength
}

type ImmutableLookup<'tk, 'tv when 'tk : struct> = 
    internal
        {
            KeyToMemoryMappedLocation: IDictionary<'tk, ValueLocationWithVariableLength list>
            OpenedFile: IOpenedFile
            KeySerialiser: IFixedSizeSerialiser<'tk>
            ValueSerialiser: IFixedSizeSerialiser<'tv>
        }
    interface IDisposable with
        member x.Dispose() = 
            x.OpenedFile.Flush()
            x.OpenedFile.Dispose()

/// Represents an immutable create only lookup structure (1 key to many elements).
/// Index entries are stored first then values meaning loading up time is loaded contiguously.
/// Entries for each key are also layed out contiguously to avoid disk seeking and cache misses (index data locality).
/// Due to knowing the contents in advance and the use of Fixed Width data types the file can be created at the exact capacity in advance.
/// Note: All keys and values must be able to be serialised to a fixed width; order inside each lookup is in FIFO order (a stack).
module ImmutableLookup =
    
    let countHeaderSize = sizeof<int64> |> int64

    let lengthFieldPointer = 0L<LocationPointer>

    let create (keySerialiser: IFixedSizeSerialiser<'tk>) (valueSerialiser: IFixedSizeSerialiser<'tv>) (openFileFactory: IFixedFileFactory) (fileLocation: string) (d: IDictionary<'tk, #ICollection<'tv>>) =
        
        let indexEntrySerialiser : IFixedSizeSerialiser<ImmutableLookupIndex> = Serialisers.Marshalling.serialiser

        let totalEntries = int64 (d |> Seq.sumBy (fun x -> int64 x.Value.Count))
        
        // The reason we want a fixed serialiser - so we can dimension the whole file capacity in advance and use fixed blocks.
        let valueSize = valueSerialiser.FixedSizeOf
        let requiredSpaceForValues = d |> Seq.sumBy (fun y -> int64 y.Value.Count * valueSize)
        let requiredSpaceForIndexes = totalEntries * indexEntrySerialiser.FixedSizeOf
        let requiredSpaceKeyKeys = totalEntries * keySerialiser.FixedSizeOf
        let requiredCapacityForIndexCount = countHeaderSize
        
        let totalCapacity = requiredCapacityForIndexCount + requiredSpaceForIndexes + requiredSpaceKeyKeys + requiredSpaceForValues
                    
        use openedFile = openFileFactory.CreateFileWithCapacity totalCapacity fileLocation

        let mutable indexOffset = locationPointer countHeaderSize
        let mutable dataOffset = indexOffset + (locationPointer (totalEntries * (indexEntrySerialiser.FixedSizeOf + keySerialiser.FixedSizeOf)))
        let mutable indexEntryCount = 0L

        for kv in d do
        for v in kv.Value do
                
            let valueSerialised = valueSerialiser.Serialise v
            let dataLength = valueSerialised.Count - valueSerialised.Offset
            let valueLocation =  { ValueLocationWithVariableLength.Length = valueSerialised.Count; ValueLocationWithVariableLength.Index = dataOffset;  }
            let indexEntry = { ValueLocationWithVariableLength = valueLocation }

            // Write the index info, then the key, then the lookup value.                    
            let indexData = indexEntrySerialiser.Serialise indexEntry
            openedFile.WriteArray indexData indexOffset
            let keyData = keySerialiser.Serialise kv.Key
            openedFile.WriteArray keyData (indexOffset + (indexEntrySerialiser.FixedSizeOf |> locationPointer))
            openedFile.WriteArray valueSerialised dataOffset
                
            indexOffset <- indexOffset + (locationPointer (indexEntrySerialiser.FixedSizeOf + keySerialiser.FixedSizeOf))
            dataOffset <- dataOffset + (locationPointer (int64 dataLength))
            indexEntryCount <- indexEntryCount + 1L

        writeInt64ToLocation openedFile lengthFieldPointer indexEntryCount

        openedFile.Flush()

    let createWithDefaultSerialiser (openFileFactory: IFixedFileFactory) (fileLocation: string) (d: IDictionary<'tk, #ICollection<'tv>>) = 
        create Serialisers.Marshalling.serialiser Serialisers.Marshalling.serialiser openFileFactory fileLocation d

    let private updateDictWithValueLocation key valueLocation (d: IDictionary<_, _ list>) = 
        match d.TryGetValue(key) with
        | (true, l) -> d.[key] <- valueLocation :: l
        | (false, _) ->  d.[key] <- [ valueLocation ]

    let inline private deserialiseFromLocation<'t> (s: ISerialiser<'t>) (va: IOpenedFile) (v: ValueLocationWithVariableLength) = 
        let arr = Array.zeroCreate<byte> (int v.Length) |> ArraySegment<_>
        MKKV.CommonUtils.deserialiseFromFile s va arr v.Index

    /// Gets the series of values from the memory mapped file given the key
    /// Note that the memory mapped file is used lazily upon evaluation of the sequence.
    let tryGetValue k (d: ImmutableLookup<'tk, 'tv>) : struct (bool * 'tv seq) = 
        match d.KeyToMemoryMappedLocation.TryGetValue(k) with
        | (true, v) -> struct (true, v |> Seq.map (fun x -> deserialiseFromLocation d.ValueSerialiser d.OpenedFile x))
        | (false, _) -> struct (false, [] :>_)

    let asSeq (d: ImmutableLookup<'tk, 'tv>) : struct ('tk * 'tv seq) seq = seq {
        for kv in d.KeyToMemoryMappedLocation do
        yield struct (kv.Key, kv.Value |> Seq.map (fun x -> deserialiseFromLocation d.ValueSerialiser d.OpenedFile x))
    }

    let openFile<'tk, 'tv when 'tk : struct and 'tk : (new: unit -> 'tk) and 'tk : equality> 
        (keySerialiser: IFixedSizeSerialiser<'tk>)
        (valueSerialiser: IFixedSizeSerialiser<'tv>) 
        (openFileFactory: IFixedFileFactory) 
        (fileLocation: string) = 
        
        let indexEntrySerialiser : IFixedSizeSerialiser<ImmutableLookupIndex> = Marshalling.serialiser
        
        let indexToLocationDict = Dictionary<'tk, ValueLocationWithVariableLength list>()
        let indexAndKeySize = indexEntrySerialiser.FixedSizeOf + keySerialiser.FixedSizeOf

        let openedFile = openFileFactory.OpenFile fileLocation

        let count = getInt64AtLocation openedFile lengthFieldPointer
        
        let indexAndKeyBuffer = ArraySegment<byte>(Array.zeroCreate<byte> (int indexAndKeySize))

        let rec read indexOffset currentCount = 

            if (currentCount = count) 
            then 
                { ImmutableLookup.KeyToMemoryMappedLocation = indexToLocationDict;
                  OpenedFile = openedFile
                  KeySerialiser = keySerialiser
                  ValueSerialiser = valueSerialiser }
            else
                
                openedFile.ReadArray indexOffset indexAndKeyBuffer

                let index = indexEntrySerialiser.Deserialise (ArraySegment<_>(indexAndKeyBuffer.Array, 0, int indexEntrySerialiser.FixedSizeOf))
                let key = keySerialiser.Deserialise (ArraySegment<_>(indexAndKeyBuffer.Array, int indexEntrySerialiser.FixedSizeOf, int keySerialiser.FixedSizeOf))

                updateDictWithValueLocation key index.ValueLocationWithVariableLength indexToLocationDict
                read (indexOffset + (locationPointer indexAndKeySize)) (currentCount + 1L)

        read (lengthFieldPointer + (locationPointer countHeaderSize)) 0L
        
    let openFileWithDefaultSerialiser<'tk, 'tv when 'tk : struct and 'tk : (new: unit -> 'tk) and 'tk : equality and 'tv : struct> (openFileFactory: IFixedFileFactory) (fileLocation: string) = 
        openFile<'tk, 'tv> Serialisers.Marshalling.serialiser Serialisers.Marshalling.serialiser openFileFactory fileLocation
    