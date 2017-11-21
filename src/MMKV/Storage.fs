namespace MMKV.Storage

open System
open System.IO.MemoryMappedFiles
open System.IO

[<Measure>] type LocationPointer

type IOpenedFile = 
    inherit IDisposable
    abstract member WriteArray: data: ArraySegment<byte> -> position: int64<LocationPointer> -> unit
    abstract member ReadArray: position: int64<LocationPointer> -> ArraySegment<byte> -> unit
    abstract member Flush: unit -> unit

/// A memory/file allocator that must be allocated with a fixed size and can't grow (e.g an array, a memory mapped file, etc).
type IFixedFileFactory =     
    abstract member CreateFileWithCapacity: capacity: int64 -> fileLocation: string -> IOpenedFile
    abstract member OpenFile: fileLocation: string -> IOpenedFile
    abstract member FileExists: fileLocation: string -> bool

/// A memory/file allocator that can create storage regions that can grow in size.
/// Normally these can also create with a minimum capacity hence provide more than a IFixedFileFactory.
type IOpenedFileFactory = 
    inherit IFixedFileFactory
    abstract member CreateFile: fileLocation: string -> IOpenedFile

/// The module containing the file factory for memory mapped files.
module MemoryMappedFileStorage = 

    type internal MemoryMappedOpenedFile = 
        { MemoryMappedFile: MemoryMappedFile
          ViewStream: MemoryMappedViewStream }
        interface IOpenedFile with
            member x.Dispose() = 
                x.ViewStream.Flush()
                x.ViewStream.Dispose()
                x.MemoryMappedFile.Dispose()
            member x.WriteArray data position = 
                x.ViewStream.Seek(int64 position, SeekOrigin.Begin) |> ignore    
                x.ViewStream.Write(data.Array, data.Offset, data.Count)
            member x.Flush() = x.ViewStream.Flush()
            member x.ReadArray position arrayToReadInto = 
                x.ViewStream.Seek(int64 position, SeekOrigin.Begin) |> ignore
                x.ViewStream.Read(arrayToReadInto.Array, arrayToReadInto.Offset, arrayToReadInto.Count) |> ignore
    
    /// Retrieves a memory mapped file IFixedFactory.
    let openFileFactory = {
        new IFixedFileFactory with
            member __.CreateFileWithCapacity capacity fileLocation = 
                let mmf = MemoryMappedFile.CreateFromFile(fileLocation, FileMode.CreateNew, null, capacity)
                let vs = mmf.CreateViewStream()
                { MemoryMappedFile = mmf; ViewStream = vs } :>_
            member __.OpenFile fileLocation = 
                let mmf = MemoryMappedFile.CreateFromFile(fileLocation, FileMode.OpenOrCreate)
                let vs = mmf.CreateViewStream()
                { MemoryMappedFile = mmf; ViewStream = vs } :>_
            member __.FileExists fileLocation = File.Exists(fileLocation)
        }

type internal StreamOpenedFile = 
    { Stream: Stream }
    interface IOpenedFile with
        member x.Dispose() = 
            x.Stream.Flush()
            x.Stream.Dispose()
        member x.WriteArray data position = 
            x.Stream.Seek(int64 position, SeekOrigin.Begin) |> ignore    
            x.Stream.Write(data.Array, data.Offset, data.Count)
        member x.Flush() = x.Stream.Flush()
        member x.ReadArray position arrayToReadInto = 
            x.Stream.Seek(int64 position, SeekOrigin.Begin) |> ignore
            x.Stream.Read(arrayToReadInto.Array, arrayToReadInto.Offset, arrayToReadInto.Count) |> ignore

/// The module containing the file factory for standard file storage.
module StandardFileStorage = 

    /// Retrieves a standard file stream backed IOpenedFileFactory.
    let openFileFactory = {
        new IOpenedFileFactory with
            member __.CreateFile fileLocation = 
                let fs = new FileStream(fileLocation, FileMode.Create)
                { Stream = fs } :>_
            member __.CreateFileWithCapacity capacity fileLocation = 
                let fs = new FileStream(fileLocation, FileMode.Create)
                fs.SetLength(capacity)
                { Stream = fs } :>_
            member __.OpenFile fileLocation = 
                let fs = new FileStream(fileLocation, FileMode.Open)
                { Stream = fs } :>_
            member __.FileExists fileLocation = File.Exists(fileLocation)            
        }

module MemoryStreamStorage =
    open System.Collections.Generic
    
    type internal StreamOpenedFile = 
        { MemoryStream: MemoryStream
          FlushData: byte[] -> unit }
        interface IOpenedFile with
            member x.Dispose() = 
                x.MemoryStream.Flush()
                x.MemoryStream.Dispose()
                x.FlushData (x.MemoryStream.ToArray())
            member x.WriteArray data position = 
                let toMove = position - (x.MemoryStream.Position * 1L<LocationPointer>)
                
                x.MemoryStream.Seek(int64 toMove, SeekOrigin.Current) |> ignore    
                x.MemoryStream.Write(data.Array, data.Offset, data.Count)
            member x.Flush() = x.MemoryStream.Flush()
            member x.ReadArray position arrayToReadInto = 
                let toMove = position - (x.MemoryStream.Position * 1L<LocationPointer>)
                x.MemoryStream.Seek(int64 toMove, SeekOrigin.Current) |> ignore    
                x.MemoryStream.Read(arrayToReadInto.Array, arrayToReadInto.Offset, arrayToReadInto.Count) |> ignore

    let createNewFileFactory() = 
        let streamData = Dictionary<string, byte[]>()

        { new IOpenedFileFactory with
            member __.CreateFile fileLocation = 
                match streamData.TryGetValue(fileLocation) with
                | (true, _) -> failwith "Trying to create a new memory stream region when one is already been created. Use another instance of MemoryMappedStreamFileFactory."
                | (false, _) -> 
                    let stream = new MemoryStream()
                    { MemoryStream = stream; FlushData = fun d -> streamData.[fileLocation] <- d } :>_
            member __.CreateFileWithCapacity capacity fileLocation =
                match streamData.TryGetValue(fileLocation) with
                | (true, _) -> failwith "Trying to create a new memory stream region when one is already been created. Use another instance of MemoryMappedStreamFileFactory."
                | (false, _) -> 
                    let stream = new MemoryStream(int capacity)
                    { MemoryStream = stream; FlushData = fun d -> streamData.[fileLocation] <- d } :>_
            member __.OpenFile fileLocation =   
                match streamData.TryGetValue(fileLocation) with
                | (true, d) -> 
                    let stream = new MemoryStream(d.Length)
                    stream.Write(d, 0, d.Length)
                    stream.Seek(0L, SeekOrigin.Begin) |> ignore
                    { MemoryStream = stream; FlushData = fun d -> streamData.[fileLocation] <- d } :>_
                | (false, _) -> failwith "File is not yet created on this memory stream factory"
            member __.FileExists fileLocation = streamData.ContainsKey(fileLocation)          
        }

/// Uses a rollover file strategy to promote a fixed allocation of data blocks into a variable length structure.
/// It does this by creating more than one file, with a rollover for different segments.
module RolloverComposedStorage = 
    open System.Collections.Generic
    
    type internal RolloverComposedStorageFile = 
        { FileFactory: IFixedFileFactory
          RolloverSize: int64
          Files: Dictionary<int64, IOpenedFile>
          FilePrefix: string
          FileLocation: string }

    let internal determineFileName fileLocation filePrefix fileNoToUse = Path.Combine(fileLocation, (sprintf "./%s-%i.rcsf" filePrefix fileNoToUse)) |> Path.GetFullPath

    let internal getFileName rcs fileNoToUse = Path.Combine(rcs.FileLocation, determineFileName rcs.FileLocation rcs.FilePrefix fileNoToUse) |> Path.GetFullPath

    let internal fileExists rcs fileNo = rcs.FileFactory.FileExists(getFileName rcs fileNo)    

    let internal getRolloverFile (rcs: RolloverComposedStorageFile) fileNoToUse = 
        match (rcs.Files.TryGetValue(fileNoToUse), fileExists rcs fileNoToUse) with
        | ((true, f), _) -> f
        | ((false, _), true) -> 
            let f = rcs.FileFactory.OpenFile (getFileName rcs fileNoToUse)
            rcs.Files.[fileNoToUse] <- f
            f
        | ((false, _), false) ->
            let filePathToUse = getFileName rcs fileNoToUse
            let f = rcs.FileFactory.CreateFileWithCapacity rcs.RolloverSize filePathToUse
            rcs.Files.[fileNoToUse] <- f
            f

    let private performOperation operation (rcs: RolloverComposedStorageFile) (data: ArraySegment<byte>) position = 
        let rec performOperationRec (currentPosition: int64<LocationPointer>) currentOffset countRemaining =
            let fileNoToUse = currentPosition / (rcs.RolloverSize * 1L<LocationPointer>)
            let positionInFile = currentPosition % (rcs.RolloverSize * 1L<LocationPointer>)
            
            let fileToUse = getRolloverFile rcs fileNoToUse

            let bytesRemainingInCurrentFile = rcs.RolloverSize - (positionInFile / 1L<LocationPointer>)
            let byteCount =  (min bytesRemainingInCurrentFile countRemaining) * 1L<LocationPointer>

            let segmentToWrite = new ArraySegment<byte>(data.Array, currentOffset, int byteCount)
            operation fileToUse currentPosition segmentToWrite
            
            let newCountRemaining = countRemaining - (int64 byteCount)
            if countRemaining <> 0L
            then performOperationRec (currentPosition + byteCount) (currentOffset + int byteCount) newCountRemaining

        performOperationRec position 0 (int64 data.Count)

    let private write = performOperation (fun (f: IOpenedFile) currentPos segment -> f.WriteArray segment currentPos)
    let private read = performOperation (fun (f: IOpenedFile) currentPos segment -> f.ReadArray currentPos segment)

    let private createOpenFileWithConfig (config: RolloverComposedStorageFile) = {
        new IOpenedFile with
            member __.Flush() = config.Files.Values |> Seq.iter (fun x -> x.Flush())
            member __.ReadArray pos data = read config data pos
            member __.WriteArray data pos = write config data pos
            member this.Dispose() = this.Flush(); config.Files.Values |> Seq.iter (fun x -> x.Dispose())
    }

    /// Retrieves a rollover factory.
    let openFileFactory (fixedFileFactory: IFixedFileFactory) filePrefix rolloverSizeInBytes = {
        new IOpenedFileFactory with
            member __.CreateFile fileLocation = 
                createOpenFileWithConfig
                    { FileFactory = fixedFileFactory
                      RolloverSize = rolloverSizeInBytes
                      Files = Dictionary<_, _>()
                      FilePrefix = filePrefix
                      FileLocation = fileLocation }
            member __.CreateFileWithCapacity capacity fileLocation = 
                createOpenFileWithConfig
                    { FileFactory = fixedFileFactory
                      RolloverSize = rolloverSizeInBytes
                      Files = Dictionary<_, _>()
                      FilePrefix = filePrefix
                      FileLocation = fileLocation }
            member __.OpenFile fileLocation = 
                createOpenFileWithConfig
                    { FileFactory = fixedFileFactory
                      RolloverSize = rolloverSizeInBytes
                      Files = Dictionary<_, _>()
                      FilePrefix = filePrefix
                      FileLocation = fileLocation }
            member __.FileExists fileLocation = fixedFileFactory.FileExists(determineFileName fileLocation filePrefix 0L)
        }
