namespace MMKV.Unit

open Expecto
open FsCheck
open MMKV.Storage
open MMKV.Serialisers
open MMKV
    
module ImmutableLookupTest =
    
   
    type [<Struct>] TestStruct = {
        NumberOne: int
        NumberTwo: int64
        NumberThree: OptionalStruct<int64>
    }

    type DataGenerator =
        static member Generate() : Arbitrary<(int * TestStruct) list> = Gen.listOfLength 10000 Arb.generate |> Arb.fromGen
    
    [<Tests>]
    let test() = 
        testCase "Immutable lookup roundtrip works" <| fun () -> 
            
            let property (typeToTest: (int * TestStruct) list) = 

                let inputDataToCompare = typeToTest |> Seq.groupBy fst |> Seq.map (fun (key, group) -> (key, group |> Seq.map snd |> Seq.toArray)) |> Seq.toArray
                let storage = MemoryStreamStorage.createNewFileFactory()
                ImmutableLookup.create Marshalling.serialiser Marshalling.serialiser storage "Dummy" (inputDataToCompare |> dict)

                use lookup = ImmutableLookup.openFile<int, TestStruct> Marshalling.serialiser Marshalling.serialiser storage "Dummy"
                try
                    let result = lookup |> ImmutableLookup.asSeq |> Seq.map (fun struct (key, list) -> (key, list |> Seq.toArray |> Array.rev)) |> Seq.toArray
                    (inputDataToCompare |> Map.ofSeq) |> Expect.equal (result |> Map.ofSeq)
                with
                | ex -> 
                    printfn "Message %s" ex.Message
                    reraise()        

            let config = { Config.QuickThrowOnFailure with Arbitrary = [ typeof<DataGenerator> ] }
            Check.One(config, property)



