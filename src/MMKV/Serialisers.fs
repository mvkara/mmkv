namespace MMKV.Serialisers

open System

/// Converts from byte representation to object and back.
/// Instantiated for a given type to avoid casting and constraints on some serialisers
type ISerialiser<'t> =
    abstract member Serialise: 't -> ArraySegment<byte>
    abstract member Deserialise: ArraySegment<byte> -> 't
    abstract member IsFixedSize: bool

/// A serialiser that can serialise a given type to a fixed width at all times.
/// Useful for structures that require known allocation sizes.
type IFixedSizeSerialiser<'t> = 
    inherit ISerialiser<'t>
    abstract member FixedSizeOf: int64

type Serialiser<'t, 'ts when 'ts :> ISerialiser<'t>> = unit -> 'ts

type CSharpSerialiserFactory<'tv, 'ts when 'ts :> ISerialiser<'tv>> = Func<unit, 'ts>

/// Contains the marshalling serialiser which by default is also used for serialising keys.
/// Effectively it is a simple mem-copy of all the fields of the structure being serialised.
/// To use this serialiser the value to be serialised must be a struct that can be sized and
/// marshalled to ummanaged memory using the System.Runtime.NativeInterop.Marshal class.
module Marshalling = 
    open System.Runtime.InteropServices

    let marshalObjectToBytes (t: 't) = 
        let size = Marshal.SizeOf<'t>(t)
        let dataArray = Array.zeroCreate<byte> size
        let ptr : IntPtr = Marshal.AllocHGlobal(size)
        
        try
            Marshal.StructureToPtr(t, ptr, true)
            Marshal.Copy(ptr, dataArray, 0, size)
            dataArray
        finally
            Marshal.FreeHGlobal(ptr)

    let unmarshalBytesToObject<'t when 't : struct> (arr: byte array) offset (count: int) = 
        let ptr = Marshal.AllocHGlobal(count)
        try
            Marshal.Copy(arr, offset, ptr, count)
            Marshal.PtrToStructure(ptr, typeof<'t>) :?> 't
        finally
            Marshal.FreeHGlobal(ptr)        

    /// The serialiser function.   
    [<GeneralizableValue>]
    let serialiser<'t when 't : struct> = 
        { 
            new IFixedSizeSerialiser<'t> with
                member __.Serialise t = marshalObjectToBytes(t) |> ArraySegment<_>
                member __.Deserialise b = unmarshalBytesToObject b.Array b.Offset b.Count
                member __.FixedSizeOf = int64 (Marshal.SizeOf<'t>())
                member __.IsFixedSize = true
        }