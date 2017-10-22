namespace MMKV.Unit

open MMKV.ImmutableLookup
open System.IO
open MKKV.Storage
open MMKV
open MKKV.Unit.Utils
open System
    
module AppendableLookupTest =
    open FsCheck
    open System.Collections.Generic
    open Expecto.Tests
    open Expecto
   
    type [<Struct>] TestStruct = {
        NumberOne: int
        NumberTwo: int64
        NumberThree: OptionalStruct<int64>
    }

    type DataGenerator =
        static member Generate() : Arbitrary<(int * TestStruct) list> = Gen.listOfLength 10 Arb.generate |> Arb.fromGen

    let commandTest() = 

        let buildInitialLookup() =
            
            let storage = MemoryStreamStorage.createNewFileFactory()
            let serialiser = Serialisers.Marshalling.serialiser

            let indexAndDataFileConfig = { IndexAndDataFileConfig.IndexFile = "IndexFile"; DataFile = "DataFile"}

            AppendableLookup.create serialiser serialiser storage indexAndDataFileConfig (Map.empty<int, List<TestStruct>>)

            AppendableLookup.openFile<int, TestStruct> serialiser serialiser storage indexAndDataFileConfig

        let addCommand k v = {
            new Command<_, _>() with
                override __.RunActual appendLookup =
                    AppendableLookup.addValueToLookup k v appendLookup; appendLookup
                override __.RunModel model = 
                    match model |> Map.tryFind k with
                    | Some(y) -> model |> Map.add k (v :: y)
                    | None -> model |> Map.add k [v]
                override __.ToString() = "addCommand" 
                override __.Post(lookup, model) =
                    // Check that immutable lookup lists are the same by key
                    let actualAsModel = lookup |> AppendableLookup.asSeq |> Seq.map (fun struct (x, y) -> (x, y |> Seq.toList)) |> Map.ofSeq
                    let result = model = actualAsModel
                    result |@ sprintf "model: %A <> %A" model actualAsModel
        }

        { new ICommandGenerator<_, _> with
            member __.InitialActual = buildInitialLookup()
            member __.InitialModel  = Map.empty 
            member __.Next model = gen {
                let! key = Gen.elements [ 1 .. 100 ]
                let! value = Arb.generate
                return addCommand key value
            }}
    
    [<Tests>]
    let test() = 
        testCase "AppendableLookup matches in-memory model" <| fun () -> 
            let config = { Config.QuickThrowOnFailure with Arbitrary = [ yield typeof<DataGenerator> ] }
            let property = Command.toProperty (commandTest())
            Check.One(config, property)