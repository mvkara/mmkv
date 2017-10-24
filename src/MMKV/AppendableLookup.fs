namespace MMKV

open System
open System.IO
open System.IO.MemoryMappedFiles
open System.Collections.Generic
open System.Runtime.InteropServices
open MKKV.Storage
open MMKV.Serialisers
open MKKV.CommonUtils
open MKKV

type [<Struct; CLIMutable>] internal AppendableLookupIndex = 
    { KeyLength: int; ValueLocation: ValueLocationWithVariableLength }

type AppendableLookup<'tk, 'tv when 'tk : equality> = 
    internal
        { KeyToMemoryMappedLocation: IDictionary<'tk, ValueLocationWithVariableLength list>
          IndexFile: IOpenedFile
          DataFile: IOpenedFile
          KeySerialiser: ISerialiser<'tk>
          ValueSerialiser: ISerialiser<'tv>
          mutable CurrentIndexFileLocation: int64<LocationPointer>
          mutable CurrentDataFileLocation: int64<LocationPointer> }
    interface IDisposable with
        member x.Dispose() = 
            x.IndexFile.Dispose()
            x.DataFile.Dispose()

/// Represents an append only lookup (1 key to many elements) layed out using a memory mapped file.
/// Note that this implementation requires two separate files; one for the index and one for the actual data
/// in order to avoid a large amount of disk seeking and better preemption when loading the index file.
/// Note: All keys must be of a fixed size and be able to be marshalled.
module AppendableLookup =
    let private sizeOfLengthHeader = sizeof<int64> |> int64
    let private indexSerialiser : IFixedSizeSerialiser<AppendableLookupIndex> = Serialisers.Marshalling.serialiser
    let private indexDataOffset = locationPointer indexSerialiser.FixedSizeOf
    let private indexEntrySize = Serialisers.Marshalling.serialiser<AppendableLookupIndex>.FixedSizeOf

    let inline private getCountOfFile (opf: IOpenedFile) = CommonUtils.getInt64AtLocation opf 0L<LocationPointer>

    let inline private writeNewCount (opf: IOpenedFile) count = 
        CommonUtils.writeInt64ToLocation opf 0L<LocationPointer> count

    let private addEntryToFiles
        (al: AppendableLookup<_, _>)
        key
        value =

        let keySerialised = al.KeySerialiser.Serialise key
        let valueSerialised = al.ValueSerialiser.Serialise value
        let keyLength = keySerialised.Count

        let indexEntry = 
            { AppendableLookupIndex.ValueLocation = 
                { ValueLocationWithVariableLength.Index = al.CurrentDataFileLocation; 
                  Length = valueSerialised.Count }
              KeyLength = keyLength } 

        // Length and position data, then key, then value in separate file.
        CommonUtils.serialiseToFile 
            indexSerialiser
            al.IndexFile 
            al.CurrentIndexFileLocation
            indexEntry

        al.IndexFile.WriteArray keySerialised (al.CurrentIndexFileLocation + indexDataOffset)
        al.DataFile.WriteArray valueSerialised al.CurrentDataFileLocation
        al.CurrentIndexFileLocation <- al.CurrentIndexFileLocation + indexDataOffset + locationPointerFromInt keySerialised.Count
        al.CurrentDataFileLocation <- al.CurrentDataFileLocation + locationPointerFromInt valueSerialised.Count
        indexEntry

    [<CompiledName("Create")>]
    let create 
        (keySerialiser: ISerialiser<'tk>)
        (valueSerialiser: ISerialiser<'tv>)
        (openFileFactory: IOpenedFileFactory) 
        (fileLocation: IndexAndDataFileConfig) 
        (initialData: IDictionary<'tk, #ICollection<'tv>> when 'tk : equality) =
        
        use openedIndexFile = openFileFactory.CreateFile fileLocation.IndexFile 
        use openedDataFile = openFileFactory.CreateFile fileLocation.DataFile 

        let mutable indexEntryCount = 0L
        let mutable indexOffset = locationPointer sizeOfLengthHeader
        let mutable dataOffset = 0L<LocationPointer>

        let dataStructure = 
            { AppendableLookup.KeyToMemoryMappedLocation = Map.empty;
              IndexFile = openedIndexFile
              DataFile = openedDataFile
              KeySerialiser = keySerialiser
              ValueSerialiser = valueSerialiser
              CurrentIndexFileLocation = indexOffset
              CurrentDataFileLocation = dataOffset }

        let entriesWrittenCount = 
            initialData 
            |> Seq.collect (fun x -> x.Value |> Seq.map (fun y -> (x.Key, y))) 
            |> Seq.fold 
                (fun currentCount (k, v) -> 
                    addEntryToFiles dataStructure k v |> ignore
                    currentCount + 1L)
                0L

        writeNewCount openedIndexFile entriesWrittenCount

    let private updateDictWithValueLocation key valueLocation (d: IDictionary<_, _ list>) = 
        match d.TryGetValue(key) with
        | (true, l) -> d.[key] <- valueLocation :: l
        | (false, _) ->  d.[key] <- [ valueLocation ]

    [<CompiledName("OpenFile")>]
    let openFile<'tk, 'tv when 'tk : equality> 
        (keySerialiser: ISerialiser<'tk>)
        (valueSerialiser: ISerialiser<'tv>) 
        (openFileFactory: IOpenedFileFactory) 
        (fileLocation: IndexAndDataFileConfig) = 

        let indexToLocationDict = Dictionary<'tk, ValueLocationWithVariableLength list>()

        let openedIndexFile = openFileFactory.OpenFile fileLocation.IndexFile
        let openedDataFile = openFileFactory.OpenFile fileLocation.DataFile

        let count = getCountOfFile openedIndexFile

        let mutable indexOffset = locationPointer sizeOfLengthHeader
        
        let mutable deserialisationBuffer = Array.zeroCreate (sizeof<AppendableLookupIndex>)

        let rec read (indexOffset: int64<LocationPointer>) lastDataLocation currentCount = 
            if currentCount = count
            then 
                
                let dataLocation =
                    match (lastDataLocation: ValueLocationWithVariableLength option) with
                    | None -> 0L<LocationPointer>
                    | Some(v) -> (v.Index + locationPointerFromInt v.Length)
                
                { AppendableLookup.KeyToMemoryMappedLocation = indexToLocationDict;
                  IndexFile = openedIndexFile
                  DataFile = openedDataFile
                  KeySerialiser = keySerialiser
                  ValueSerialiser = valueSerialiser
                  CurrentIndexFileLocation = indexOffset
                  CurrentDataFileLocation = dataLocation }
            else
                let lengthAndPosData : AppendableLookupIndex = 
                    CommonUtils.deserialiseFromFile 
                        indexSerialiser 
                        openedIndexFile
                        (ArraySegment<_>(deserialisationBuffer, 0, int indexSerialiser.FixedSizeOf))
                        indexOffset

                if deserialisationBuffer.Length < lengthAndPosData.KeyLength
                    then deserialisationBuffer <- Array.zeroCreate lengthAndPosData.KeyLength

                let key = 
                    CommonUtils.deserialiseFromFile 
                        keySerialiser
                        openedIndexFile 
                        (ArraySegment<_>(deserialisationBuffer, 0, lengthAndPosData.KeyLength))
                        (indexOffset + (locationPointer indexSerialiser.FixedSizeOf))

                if deserialisationBuffer.Length < lengthAndPosData.ValueLocation.Length
                    then deserialisationBuffer <- Array.zeroCreate lengthAndPosData.ValueLocation.Length

                updateDictWithValueLocation key lengthAndPosData.ValueLocation indexToLocationDict
                
                read 
                    (indexOffset + (locationPointer indexSerialiser.FixedSizeOf) + (lengthAndPosData.KeyLength |> int64 |> locationPointer))
                    (Some lengthAndPosData.ValueLocation)
                    (currentCount + 1L)
                
        read indexOffset None 0L        

    let inline private deserialiseValueFromLocation (vs: ISerialiser<_>) (openedFile: IOpenedFile) (v: ValueLocationWithVariableLength) = 
        let arr = ArraySegment<byte>(Array.zeroCreate<byte> v.Length)
        openedFile.ReadArray v.Index arr
        vs.Deserialise arr 

    /// Gets the series of values from the memory mapped file given the key
    /// Note that the memory mapped file is used lazily upon evaluation of the sequence.
    [<CompiledName("TryGetValue")>]
    let tryGetValue k (d: AppendableLookup<'tk, 'tv>) : struct (bool * 'tv seq) = 
        match d.KeyToMemoryMappedLocation.TryGetValue(k) with
        | (true, v) -> struct (true, v |> Seq.map (fun location -> deserialiseValueFromLocation d.ValueSerialiser d.DataFile location))
        | (false, _) -> struct (false, [] :>_)

    let private getIndexAtPointer (al: AppendableLookup<_, _>) indexOffset = 
        let s : IFixedSizeSerialiser<AppendableLookupIndex> = Serialisers.Marshalling.serialiser 
        let deserialisationBuffer = ArraySegment<_>(Array.zeroCreate (int s.FixedSizeOf))
        
        CommonUtils.deserialiseFromFile 
            s
            al.IndexFile
            deserialisationBuffer
            indexOffset

    [<CompiledName("AddValueToLookup")>]
    /// Adds a value to the sequence at the provided key.
    let addValueToLookup k v (d: AppendableLookup<'tk, 'tv>) = 

        let indexEntry = addEntryToFiles d k v

        writeNewCount d.IndexFile ((getCountOfFile d.IndexFile) + 1L)

        let listToAddTo = 
            match d.KeyToMemoryMappedLocation.TryGetValue(k) with
            | (true, currentList) -> currentList
            | (false, _) -> List.empty

        d.KeyToMemoryMappedLocation.[k] <- indexEntry.ValueLocation :: listToAddTo

    [<CompiledName("AsSeq")>]
    let asSeq (d: AppendableLookup<'tk, 'tv>) : struct ('tk * 'tv seq) seq = seq {
        for kv in d.KeyToMemoryMappedLocation do
        yield struct (kv.Key, kv.Value |> Seq.map (fun x -> deserialiseValueFromLocation d.ValueSerialiser d.DataFile x))
    }




        
