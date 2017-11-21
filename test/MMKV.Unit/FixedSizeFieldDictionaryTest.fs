namespace MMKV.Unit

open MMKV.Storage
open MMKV
open System
open FsCheck
open Expecto.Tests
open Expecto
    
module FixedSizeDictionaryTest =
    
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
            FixedSizeFieldDictionary.create serialiser serialiser storage "TEST" (Map.empty<int, TestStruct>)
            FixedSizeFieldDictionary.openFile<int, TestStruct> serialiser serialiser storage "TEST"

        let compareActualToModel lookup model = 
            // Check that immutable lookup lists are the same by key
            let actualAsModel = lookup |> FixedSizeFieldDictionary.asSeq |> Seq.map (fun struct (x, y) -> (x, y)) |> Map.ofSeq
            let result = model = actualAsModel
            result |@ sprintf "model: %A <> %A" model actualAsModel

        let addCommand k v = {
            new Command<_, _>() with
                override __.RunActual d =
                    FixedSizeFieldDictionary.add k v d; d
                override __.RunModel model = model |> Map.add k v
                override __.ToString() = sprintf "addCommand [Key: %A, Value: %A]" k v 
                override __.Post(lookup, model) = compareActualToModel lookup model    
        }

        let removeCommand k = {
            new Command<_, _>() with
                override __.RunActual d = FixedSizeFieldDictionary.remove k d; d
                override __.RunModel model = model |> Map.remove k
                override __.ToString() = sprintf "removeCommand [Key: %A]" k
                override __.Post(lookup, model) = compareActualToModel lookup model      
        }

        { new ICommandGenerator<FixedSizeFieldDictionary<int, TestStruct>, Map<int, TestStruct>> with
            member __.InitialActual = buildInitialLookup()
            member __.InitialModel  = Map.empty 
            member __.Next model = 
                let addCommandGen = gen {
                    let! key = Gen.elements [ 1 .. 100 ]
                    let! value = Arb.generate
                    return addCommand key value
                }

                let removeCommandGen = gen {
                    let! key = Gen.elements [ 1 .. 100 ]
                    return removeCommand key
                }

                Gen.oneof [ addCommandGen; removeCommandGen ]
        }
    
    [<Tests>]
    let tests() = 
        testCase "FixedSizeFieldDictionary matches in-memory map" <| fun () ->
            let config = { Config.QuickThrowOnFailure with Arbitrary = [ yield typeof<DataGenerator> ]; MaxTest = 5 }
            let property = Command.toProperty (commandTest())
            Check.One(config, property)