# MMKV (Memory Mapped Key Value)

## Purpose

This library contains key-value data structures powered under the hood via a storage mechanism of choice.

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
Storage options can be found in the MKKV.Storage namespace. 
Each module has the property "openFileFactory" which can be used to select the backing store for your selected collection type.

- StandardFileStorage
  - Uses a standard file on disk as the backing store.

- MemoryMappedFileStorage
  - Uses a file mapped into a region of memory.
  - File must be given a fixed capacity.

- MemoryStreamStorage
  - Uses standard MemoryStream's as the backing store. Not intended for typical use; rather as a backing store useful for testing.

## How to use

1. Pick the collection for your use case in the appropriate module.
2. Pick the backing storage the collection will use for its data (in the MKKV.Storage namespace)
3. Run the appropriate create method inside the module matching the collection name.

eg. for ImmutableLookup

```
open MKKV
ImmutableLookup.create keySerialiser valueSerialiser "./file.dat" sourceData
```

4. Open the collection using the openFile method in the appropriate module for your collection.

eg. for ImmutableLookup

```
let collection = ImmutableLookup.openFile<KeyType, ValueType> keySerialiser valueSerialiser "./file.dat"
```

4. Use the static methods inside the module for that collection to interact with it.

```
let values : ValueType seq = ImmutableLookup.tryGetValue 3
```
