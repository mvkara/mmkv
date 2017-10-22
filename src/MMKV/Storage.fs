namespace MKKV.Storage

open System
open System.IO.MemoryMappedFiles
open System.IO

[<Measure>] type LocationPointer

type IOpenedFile = 
    inherit IDisposable
    abstract member WriteArray: data: ArraySegment<byte> -> position: int64<LocationPointer> -> unit
    abstract member ReadArray: position: int64<LocationPointer> -> ArraySegment<byte> -> unit
    abstract member Flush: unit -> unit

// A file that supports direct memory mapping of structs (e.g memory mapped files)
type IStructStorageFile = 
    inherit IOpenedFile
    abstract member Read<'t when 't : (new: unit -> 't) and 't : struct and 't :> ValueType> : position: int64<LocationPointer> -> 't
    abstract member Write<'t when 't : struct and 't : (new: unit -> 't) and 't :> ValueType> : data: 't -> position: int64<LocationPointer> -> unit

/// A memory/file allocator that must be allocated with a fixed size and can't grow (e.g an array, a memory mapped file, etc).
type IFixedFileFactory =     
    abstract member CreateFileWithCapacity: capacity: int64 -> fileLocation: string -> IOpenedFile
    abstract member OpenFile: fileLocation: string -> IOpenedFile

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


    type MemoryStreamFileFactory() = 
        let streamData = Dictionary<string, byte[]>()

        interface IOpenedFileFactory with
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

    let createNewFileFactory() = MemoryStreamFileFactory()

(*
/// Uses a rollover file strategy to promote a fixed allocation of data blocks into a variable length structure.
/// It does this by creating more than one file, with a rollover for different segments.
module RolloverComposedStorage = 

    type internal RolloverComposedStorageFile = 
        { FileFactory: IFixedFileFactory
          RolloverSize: int64<LocationPointer>
           }
        interface IOpenedFile with
            member x.Dispose() = 
                x.FileStream.Flush()
                x.FileStream.Dispose()
            member x.WriteArray data position = 
                x.FileStream.Seek(int64 position, SeekOrigin.Begin) |> ignore    
                x.FileStream.Write(data.Array, data.Offset, data.Count)
            member x.Flush() = x.FileStream.Flush()
            member x.ReadArray position arrayToReadInto = 
                x.FileStream.Seek(int64 position, SeekOrigin.Begin) |> ignore
                x.FileStream.Read(arrayToReadInto.Array, arrayToReadInto.Offset, arrayToReadInto.Count) |> ignore

    /// Retrieves a rollover factory.
    let openFileFactory (fixedFileFactory: IFixedFileFactory) rolloverSizeInBytes = {
        new IOpenedFileFactory with
            member __.CreateFile fileLocation = 
                let fs = new FileStream(fileLocation, FileMode.Create)
                { FileStream = fs } :>_
            member __.CreateFileWithCapacity capacity fileLocation = 
                let fs = new FileStream(fileLocation, FileMode.Create)
                fs.SetLength(capacity)
                { FileStream = fs } :>_
            member __.OpenFile fileLocation = 
                let fs = new FileStream(fileLocation, FileMode.Open)
                { FileStream = fs } :>_
        }

*)