# MMKV

## Purpose

This library contains data structures powered under the hood via a storage mechanism of choice. It aims to decouple the data structure selected from the memory/data storage used to store its contents.

## Use cases

Some example use cases include:

- Large static data files that need to be partioned/grouped/pivoted by a key (recommend: ImmutableLookup)
- Event sourced bounded contexts (DDD) where a given ID has many events appended to each ID index for folding/aggregation. (recommended: AppendableLookup)
- A large dictionary that's better on disk vs memory
- Where you may normally want to use a database but need something more performant and lighter.

## Collection Types

- AppendableLookup
  - A one-to-many data structure that allows appends on new objects per key.

- ImmutableLookup
  - A static one-to-many data structure that once created can't be changed.
  - File is sized to exactly the required capacity for the data set.

- FixedSizeFieldDictionary
  - A mutable one-to-one key-value data structure similar to a standard .NET Dictionary with fixed size regions for both key and value pairs.
  - Removal of an key moves the last inserted element into its place.

## Current file types

All collections in this library use the given IOpenFileFactory as their data backing store.
Storage options can be found in the MMKV.Storage namespace. 
Each module has the property "openFileFactory" which can be used to select the backing store for your selected collection type.
Note that some collection types can only work with fixed allocation blocks of contigious data, others require a dynamic growing data store.

- StandardFileStorage (variable length allocation)
  - Uses a standard file on disk as the backing store.

- MemoryMappedFileStorage (fixed length allocation)
  - Uses a file mapped into a region of memory.
  - File must be given a fixed capacity.

- MemoryStreamStorage (variable length allocation)
  - Uses standard MemoryStream's as the backing store. Not intended for typical use; rather as a backing store useful for testing.

- RolloverComposedStorage (variable length allocation)
  - Allows the use of fixed allocation storage formats in variable length structures (e.g structures requiring more than a IFixedFileFactory).
    It does so by allocation multiple fixed size files to simulate a larger variable length file. It composes over an underlying IFixedFileFactory.
  - As an example of what this allows you can create a AppendableLookup backed by many fixed width memory mapped files using this.

## Serialisers

To work with the file storage layer you must have a serialiser that is compatible with the types you wish to store. Any serialiser must implement the
MMKV.Serialisers.ISerialiser<_> interface.

Note: The Marshalling serialiser is provided as a default option inside the MMKV.Serialisers namespace which uses .NET marshalling under the hood.
It is compatible with specifically formatted .NET structs and is reasonably fast.


## How to use

1. Pick the collection for your use case in the appropriate module.
2. Pick the backing storage the collection will use for its data (in the MMKV.Storage namespace)

```
open MMKV
let storage = Storage.MemoryStreamStorage.createNewFileFactory()
```

3. Pick the serialiser that you wish to use for both the key and value data types.

```
let keySerialiser = Serialisers.Marshalling.serialiser
let valueSerialiser = Serialisers.Marshalling.serialiser
```

3. Run the appropriate create method inside the module matching the collection name.

eg. for ImmutableLookup

```
ImmutableLookup.create keySerialiser valueSerialiser storage "./file.dat" sourceData
```

4. Open the collection using the openFile method in the appropriate module for your collection.

eg. for ImmutableLookup

```
let collection = ImmutableLookup.openFile<int, TestRecord> keySerialiser valueSerialiser storage "./file.dat"
```

4. Use the static methods inside the module(F#)/static class(C#) for that collection to interact with it.

```
let values : ValueType seq = ImmutableLookup.tryGetValue 3
```

## Building the library

The build is compatible with the standard dotnet cli tooling. 

- Simply run "dotnet build" to build the solution.
- Run the console app under test/MMKV.Unit to run unit tests (i.e using dotnet run).
- Run the "paket.exe pack ${location}" command to create the Nuget package and output it to the location specified.