namespace MMKV

open System
open System.Collections.Generic
open MKKV.Storage
open MMKV.Serialisers
open MKKV.CommonUtils
open MKKV

type [<Struct; CLIMutable>] FileBackedDictionaryIndex = {
    CurrentLocation: ValueLocationWithFixedLength
}

type FixedSizeFieldDictionary<'tk, 'tv> = 
    internal
        {
            KeyToMemoryMappedLocation: IDictionary<'tk, ValueLocationWithFixedLength>
            OpenedFile: IOpenedFile
            KeySerialiser: IFixedSizeSerialiser<'tk>
            ValueSerialiser: IFixedSizeSerialiser<'tv>
            mutable LastPointer: int64<LocationPointer>
            mutable Count: int64
        }
    interface IDisposable with
        member x.Dispose() = 
            x.OpenedFile.Flush()
            x.OpenedFile.Dispose()

/// Represents a mutable dictionary backed by a file.
/// The precondition is that both keys and values stored in this dictionary are of a fixed size.
/// This allows a very optional usage of memory without the need for compaction of the file data on 
/// element removal.
module FixedSizeFieldDictionary =
    
    //let indexSerialiser : IFixedSizeSerialiser<FileBackedDictionaryIndex> = Serialisers.Marshalling.serialiser

    let inline private blockSizeOffset (d: FixedSizeFieldDictionary<_, _>) = d.KeySerialiser.FixedSizeOf + d.ValueSerialiser.FixedSizeOf |> locationPointer

    let deserialiseObject (serialiser: IFixedSizeSerialiser<_>) (openedFile: IOpenedFile) serialisationBuffer indexOffset  = 
        openedFile.ReadArray indexOffset serialisationBuffer
        serialiser.Deserialise serialisationBuffer

    let create 
        (keySerialiser: IFixedSizeSerialiser<_>)
        (valueSerialiser: IFixedSizeSerialiser<_>)
        (openFileFactory: IOpenedFileFactory) 
        (fileLocation: string) 
        (d: IDictionary<'tk, 'tv>)  =
            
        use openedFile = openFileFactory.CreateFile fileLocation

        let count = d.Count

        CommonUtils.writeInt64ToLocation openedFile 0L<LocationPointer> (int64 count)

        let mutable offset = locationPointer <| int64 sizeof<int64>

        let inline incrementOffset (serialiser: IFixedSizeSerialiser<_>) = 
            offset <- offset + (locationPointer serialiser.FixedSizeOf)

        let mutable currentIndex = 0
        for kv in d do
            
            let keySerialised = keySerialiser.Serialise kv.Key
            let valueSerialised = valueSerialiser.Serialise kv.Value

            let hkv = { CurrentLocation = { ValueLocationWithFixedLength.Index = offset }; }
            
            openedFile.WriteArray keySerialised offset
            incrementOffset keySerialiser
            
            openedFile.WriteArray valueSerialised offset
            incrementOffset valueSerialiser            

            currentIndex <- currentIndex + 1
        openedFile.Flush()

    let openFile<'tk, 'tv when 'tk : equality> 
        (keySerialiser: IFixedSizeSerialiser<'tk>) 
        (valueSerialiser: IFixedSizeSerialiser<'tv>) 
        (openFileFactory: IOpenedFileFactory) (fileLocation: string) = 
        
        let indexToLocationDict = Dictionary<_, _>()
        let openedFile = openFileFactory.OpenFile fileLocation
        let count = getInt64AtLocation openedFile 0L<LocationPointer>
        let blockSize = keySerialiser.FixedSizeOf + valueSerialiser.FixedSizeOf |> locationPointer
        let serialisationBuffer = Array.zeroCreate (int blockSize) |> ArraySegment<_>
        
        let rec read currentCount indexOffset = 
            if (currentCount >= count)
            then 
                { FixedSizeFieldDictionary.KeyToMemoryMappedLocation = indexToLocationDict;
                  OpenedFile = openedFile
                  KeySerialiser = keySerialiser
                  ValueSerialiser = valueSerialiser
                  LastPointer = indexOffset
                  Count = count }
            else 
                
                let key = 
                    CommonUtils.deserialiseFromFile
                        keySerialiser
                        openedFile
                        serialisationBuffer
                        indexOffset

                indexToLocationDict.[key] <- { Index = indexOffset }
                read (currentCount + 1L) (indexOffset + blockSize)
                
        read 0L (locationPointer <| int64 sizeof<int64>)
    
    let private readValueFromLocation (d: FixedSizeFieldDictionary<_, _>) locationPointerOfBlock = 
        let serialisationBuffer = Array.zeroCreate (int d.ValueSerialiser.FixedSizeOf) |> ArraySegment<_>
        CommonUtils.deserialiseFromFile d.ValueSerialiser d.OpenedFile serialisationBuffer (locationPointerOfBlock + locationPointer d.KeySerialiser.FixedSizeOf)

    let tryGetValue k (d: FixedSizeFieldDictionary<'tk, 'tv>) = 
        match d.KeyToMemoryMappedLocation.TryGetValue(k) with
        | (true, v) -> 
            // TODO: Pool the buffer between reads still allowing thread safety.
            Some(readValueFromLocation d v.Index)
        | (false, _) -> None    
        
    let inline private getLastElementPointer (d: FixedSizeFieldDictionary<_, _>) = d.LastPointer - (blockSizeOffset d)

    let remove (k: 'tk) (d: FixedSizeFieldDictionary<'tk, 'tv>) = 
        
        let blockSize = blockSizeOffset d

        let inline removeCommon() = 
            d.LastPointer <- d.LastPointer - blockSize
            CommonUtils.writeInt64ToLocation d.OpenedFile 0L<LocationPointer> (d.Count - 1L)
            d.Count <- d.Count - 1L
            d.KeyToMemoryMappedLocation.Remove(k) |> ignore
        
        let swapPositionForLastBlock foundLocationOfKey =
            
            // Get data on last position
            let buffer = ArraySegment<_>(Array.zeroCreate (int blockSize))
            d.OpenedFile.ReadArray (getLastElementPointer d) buffer
            let keyOfLastEntry = d.KeySerialiser.Deserialise (subBuffer 0L d.KeySerialiser.FixedSizeOf buffer)
            
            // Write the whole buffer of the last element into the place of the entry being removed
            d.OpenedFile.WriteArray buffer foundLocationOfKey.Index 
            
            let newLocation = { ValueLocationWithFixedLength.Index = foundLocationOfKey.Index }

            // Update the location of the element swapped from the end
            d.KeyToMemoryMappedLocation.[keyOfLastEntry] <- newLocation
                

        if d.Count = 0L then ()
        else
            match d.KeyToMemoryMappedLocation.TryGetValue(k) with
            | (true, p) when (getLastElementPointer d) = p.Index -> 
                // No need to swap the last for the current entry being removed as we are removing the last one
                removeCommon()
            | (true, p) ->
               swapPositionForLastBlock p
               removeCommon()
            | (false, _) -> ()
        
    let add k v (d: FixedSizeFieldDictionary<'tk, 'tv>) = 
        
        let addInternal locationToSaveEntryTo =
            
            let location = { ValueLocationWithFixedLength.Index = locationToSaveEntryTo }
            CommonUtils.serialiseToFile d.KeySerialiser d.OpenedFile locationToSaveEntryTo k
            CommonUtils.serialiseToFile d.ValueSerialiser d.OpenedFile (locationToSaveEntryTo + locationPointer d.KeySerialiser.FixedSizeOf) v
            d.KeyToMemoryMappedLocation.[k] <- location

        match d.KeyToMemoryMappedLocation.TryGetValue(k) with
        | (true, p) -> addInternal p.Index
        | (false, _) -> 
            addInternal d.LastPointer
            d.Count <- d.Count + 1L
            CommonUtils.writeInt64ToLocation d.OpenedFile 0L<LocationPointer> (int64 d.Count)
            d.LastPointer <- d.LastPointer + blockSizeOffset d

    let asSeq (d: FixedSizeFieldDictionary<'tk, 'tv>) : struct ('tk * 'tv) seq = seq {
        for kv in d.KeyToMemoryMappedLocation do
        yield struct (kv.Key, readValueFromLocation d kv.Value.Index)
    }