namespace MMKV

open System.IO.MemoryMappedFiles
open System
open System.Collections.Generic
open MMKV.Serialisers
open MKKV.Storage

/// A convienent wrapper for allowing null values inside a marshallable struct.
/// It allows optional fields and their optionality to be serialised inside the dictionary if required.
type [<Struct>] OptionalStruct<'t when 't : struct> = 
    | SomeStruct of 't
    | NoneStruct

/// Used for appenders which have a separate file for indexing and data.
type public IndexAndDataFileConfig = {
    /// The index file containing the keys and the lookup locations of all values for that key
    IndexFile: string
    /// Contains the actual data layed out in append order
    DataFile: string
}

type [<Struct; CLIMutable>] ValueLocationWithVariableLength = 
    { Index: int64<LocationPointer>; Length: int32 }
    static member Zero() = { Index = 0L<LocationPointer>; Length = 0 }

type [<Struct; CLIMutable>] ValueLocationWithFixedLength = {
    Index: int64<LocationPointer>    
}