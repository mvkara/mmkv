#load "CommonTypes.fs"
#load "MemoryMappedDict.fs"
open MMKV
open System
open System.Runtime.InteropServices
open System.IO

if (File.Exists("./file.dat")) then File.Delete("./file.dat")

(* can't get this working with a string representation *)

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


[ (1, record1); (3, record2)] |> dict |> MemoryMappedDict.create "./file.dat"

let fileDict = MemoryMappedDict.openFile<int, DemographicRecord> "./file.dat"

let r = fileDict |> MemoryMappedDict.tryGetValue 4

fileDict |> MemoryMappedDict.remove 4
fileDict |> MemoryMappedDict.add 4 { record2 with ID = 4 }

//dispose fileDict
