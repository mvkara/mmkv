module MMKV.Unit.TestRunner

open Expecto
open MMKV.Unit

[<EntryPoint>]
let main argv =
    printfn "Running tests..."
    Tests.runTestsInAssembly defaultConfig argv