#load "Serialisers.fs"
#load "Storage.fs"
#load "CommonTypes.fs"
#load "CommonUtils.fs"
#load "AppendableLookup.fs"
open MMKV
open System
open System.Runtime.InteropServices
open System.IO

if (File.Exists("./file.dat")) then File.Delete("./file.dat")
if (File.Exists("./index.dat")) then File.Delete("./index.dat")

let dispose x = (x :> IDisposable).Dispose()

type [<Struct; StructLayout(LayoutKind.Sequential, Pack = 1, CharSet = CharSet.Unicode)>] TestRecord = {
    ID: int
    OtherId: int
    EditionId: uint32
    [<MarshalAs(UnmanagedType.ByValTStr, SizeConst = 30)>] 
    Label: string
    [<MarshalAs(UnmanagedType.ByValTStr, SizeConst = 30)>] 
    OtherLabel: string
    Value: OptionalStruct<double>
    Total: OptionalStruct<double>
    Rank: int64
}

let record1 = 
    { 
    ID = 1;
    OtherId = 2
    EditionId = 2u
    Label = "ItemLabel"
    OtherLabel = "Composition"
    Value = SomeStruct(9.0)
    Total = SomeStruct(8.0)
    Rank = 1L
    }

let record2 = 
    { 
    ID = 2;
    OtherId = 3
    EditionId = 3u
    Label = "ItemLabel1"
    OtherLabel = "Composition1"
    Value = SomeStruct(19.0)
    Total = SomeStruct(18.0)
    Rank = 11L
    }

let record3 = { record2 with ID = 4 }

let sourceData = [ (1, [|record1|]); (3, [|record2; record3|])] |> dict 

let fileConfig = { 
    IndexAndDataFileConfig.DataFile = "./data.dat"
    IndexAndDataFileConfig.IndexFile = "index.dat" }

let maxSizeOfFile = Marshal.SizeOf<TestRecord>()

let serialiser = Serialisers.Marshalling.serialiser
let storage = MKKV.Storage.MemoryStreamStorage.createNewFileFactory()

AppendableLookup.create 
    Serialisers.Marshalling.serialiser
    Serialisers.Marshalling.serialiser
    storage 
    fileConfig 
    sourceData

let fileDict = 
    AppendableLookup.openFile<int, TestRecord>
        Serialisers.Marshalling.serialiser
        Serialisers.Marshalling.serialiser 
        storage
        fileConfig

fileDict |> AppendableLookup.tryGetValue 3

fileDict |> AppendableLookup.addValueToLookup 3 { record3 with Label = "ADDED!" }
