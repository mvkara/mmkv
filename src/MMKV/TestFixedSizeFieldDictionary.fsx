#load "Serialisers.fs"
#load "Storage.fs"
#load "CommonTypes.fs"
#load "CommonUtils.fs"
#load "FixedSizeFieldDictionary.fs"
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

type [<Struct; StructLayout(LayoutKind.Sequential, Pack = 1, CharSet = CharSet.Ansi)>] TestStruct = 
    struct
        [<MarshalAs(UnmanagedType.LPStr)>] 
        val Test: string
    end

let record3 = { record2 with ID = 4 }

let sourceData = [ (1, record1); (2, { record2 with ID = record2.ID + 1}) ] |> dict 

let storageMechanism = MMKV.Storage.MemoryStreamStorage.createNewFileFactory()

let dummyFileName = "./file.dat"

FixedSizeFieldDictionary.create Serialisers.Marshalling.serialiser Serialisers.Marshalling.serialiser storageMechanism dummyFileName Map.empty//sourceData
printfn "File created"

let fileDict = 
    FixedSizeFieldDictionary.openFile<int, TestRecord> 
        Serialisers.Marshalling.serialiser 
        Serialisers.Marshalling.serialiser 
        storageMechanism 
        dummyFileName

  
        

fileDict |> FixedSizeFieldDictionary.tryGetValue 3

fileDict |> FixedSizeFieldDictionary.add 1 { record2 with ID = 1 }
fileDict |> FixedSizeFieldDictionary.add 3 { record2 with ID = 10 }

fileDict |> FixedSizeFieldDictionary.asSeq |> Seq.map (fun struct (x, y) -> x, y) |> Map.ofSeq

fileDict |> FixedSizeFieldDictionary.remove 1

System.BitConverter.ToInt32([|3uy; 0uy ; 0uy ; 0uy|], 0)
//dispose fileDict
