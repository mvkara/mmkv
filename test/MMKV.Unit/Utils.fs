module MKKV.Unit.Utils

open System
open System.IO

type ITempFile =
    inherit IDisposable
    abstract member DirectoryPath : string

let getTempDir() = 
    let tempDirPath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()) |> Path.GetFullPath
    Directory.CreateDirectory(tempDirPath) |> ignore
    { new ITempFile with 
        member x.DirectoryPath = tempDirPath
        member x.Dispose() = Directory.GetFiles(tempDirPath) |> Seq.iter (File.Delete); Directory.Delete(tempDirPath, true) }

