module bacpac;

import std.array : appender;
import std.bitmanip : read, Endian;
import std.conv : to;
import std.datetime : Date, DateTime, dur, Duration, TimeOfDay;
import std.exception : enforce;
import std.file : read;
import std.int128 : Int128;
import std.string : assumeUTF, startsWith;
import std.traits : getUDAs, hasMember, isCallable, isInstanceOf, TemplateArgsOf;
import std.typecons : Nullable;
import std.uuid : UUID;
import std.zip : ArchiveMember, ZipArchive, ZipException;

// Attribute to describe a source BACPAC table.
struct Table
{
    string fullName;
    string fileNamePrefix;
    Column[] columns;
}

// Attribute to describe a source BACPAC column.
struct Column
{
    enum Type
    { 
        integer8, integer16, integer32, integer64, float32, float64, 
        decimal, smallmoney, money, 
        date, datetime, datetime2, datetimeoffset, smalldatetime, time, 
        boolean, uniqueidentifier, rowversion, binary, varbinary, image, geography, geometry, hierarchyid, variant, 
        fixedchar, fixednchar, varchar, nvarchar, varcharmax, nvarcharmax, text, ntext, xml, 
    }

    string fullName;
    string fieldName;
    Type type;
    bool isNullable;
    int length;
}

// Encapsulates a BACPAC decimal value.  Note, this is incomplete and only provides access to the raw data.
struct Decimal
{
    ubyte precision;
    ubyte scale;
    bool isPositive;
    Int128 baseValue;
}

// Provides run-time access into a BACPAC file.  The typical use case is to create one of these and call 
// readFullTable once for each table you require data for.
final class Bacpac
{
    string bacpacFileName;
    ZipArchive bacpacInnerFiles;
    void delegate(string) logger;
    
    this(string bacpacFileName, void delegate(string) logger) @trusted
    {
        this.bacpacFileName = bacpacFileName;
        this.logger = logger;
        
        try
            bacpacInnerFiles = new ZipArchive(read(bacpacFileName));
        catch (ZipException)
            throw new Exception(
                "Cannot process " ~ bacpacFileName ~ " because this file was not a supported zip archive." ~
                "If this is a genuine BACPAC file, the error may be because the archive uses advanced features" ~
                "not supported by the D Zip library.");
    }
    
    public auto readInnerFile(string filename) @trusted =>
        decompress(bacpacInnerFiles.directory[filename]);
    
    private auto decompress(ArchiveMember archiveMember) @trusted =>
        cast(const(ubyte)[])bacpacInnerFiles.expand(archiveMember);
    
    public auto getFiles(string filenamePrefix) @trusted
    {
        auto files = appender!(ArchiveMember[]);
        
        foreach (archiveFileName, archiveMember; bacpacInnerFiles.directory)
            if (archiveFileName.startsWith(filenamePrefix))
                files ~= archiveMember;
        
        return files;
    }
    
    public auto readFullTable(TRecord)()
    {
        enum tableAttributes = getUDAs!(TRecord, Table);
        static assert (tableAttributes.length == 1, "Record type ", table.name, " should have the Table attribute exactly once.");
        enum table = tableAttributes[0];
        
        auto records = appender!(const(TRecord)[]);
        
        foreach (file; getFiles(table.fileNamePrefix))
        {
            scope (failure) logger("Error while reading file " ~ file.name ~ ".");
        
            auto remainingData = decompress(file);
            const allData = remainingData;
            while (remainingData.length > 0)
            {
                scope (failure)
                {
                    logger("Error at offset " ~ (allData.length - remainingData.length).to!string ~ ".");
                    logger("Error while reading record " ~ (records[].length + 1).to!string ~ " (first is 1).");
                }
                
                auto consume(T)() => read!(T, Endian.littleEndian)(remainingData);
                
                auto consumeUnknownBytes(ulong lengthInBytes)
                {
                    auto data = remainingData[0 .. lengthInBytes];
                    remainingData = remainingData[lengthInBytes .. $];
                    
                    return data;
                }
                
                auto consumeBytes(int lengthToRead, int lengthOfTarget)(ref ubyte[lengthOfTarget] destination)
                {
                    static assert(lengthToRead <= lengthOfTarget);
                    
                    destination[0 .. lengthToRead] = remainingData[0 .. lengthToRead];
                    remainingData = remainingData[lengthToRead .. $];
                }
                
                TRecord record;
                
                static foreach (column; table.columns)
                {{
                    scope (failure) logger("Error while reading value for column " ~ column.fullName ~ ".");
                    
                    // A length header is necessary if the type is nullable as -1 indicates NULL and no following bytes.
                    // Some types are variable length and require the length even if the field is mandatory.
                    // Some types force a length header even when the field is mandatory and with a known length.
                    enum hasHeader = 
                        column.isNullable || 
                        column.type == Column.Type.boolean || 
                        column.type == Column.Type.decimal || 
                        column.type == Column.Type.uniqueidentifier || 
                        column.type == Column.Type.variant || 
                        column.type == Column.Type.rowversion || 
                        column.type == Column.Type.hierarchyid || 
                        column.type == Column.Type.geography || 
                        column.type == Column.Type.geometry || 
                        column.type == Column.Type.text || 
                        column.type == Column.Type.ntext || 
                        column.type == Column.Type.varchar || 
                        column.type == Column.Type.nvarchar || 
                        column.type == Column.Type.varcharmax || 
                        column.type == Column.Type.nvarcharmax || 
                        column.type == Column.Type.xml || 
                        column.type == Column.Type.fixednchar ||
                        column.type == Column.Type.image || 
                        column.type == Column.Type.binary || 
                        column.type == Column.Type.varbinary;
                    
                         static if (column.type == Column.Type.integer8        ) enum headerLength = 1;
                    else static if (column.type == Column.Type.integer16       ) enum headerLength = 1;
                    else static if (column.type == Column.Type.integer32       ) enum headerLength = 1;
                    else static if (column.type == Column.Type.integer64       ) enum headerLength = 1;
                    else static if (column.type == Column.Type.float32         ) enum headerLength = 1;
                    else static if (column.type == Column.Type.float64         ) enum headerLength = 1;
                    else static if (column.type == Column.Type.decimal         ) enum headerLength = 1;
                    else static if (column.type == Column.Type.smallmoney      ) enum headerLength = 1;
                    else static if (column.type == Column.Type.money           ) enum headerLength = 1;
                    else static if (column.type == Column.Type.date            ) enum headerLength = 1;
                    else static if (column.type == Column.Type.datetime        ) enum headerLength = 1;
                    else static if (column.type == Column.Type.datetime2       ) enum headerLength = 1;
                    else static if (column.type == Column.Type.datetimeoffset  ) enum headerLength = 1;
                    else static if (column.type == Column.Type.smalldatetime   ) enum headerLength = 1;
                    else static if (column.type == Column.Type.time            ) enum headerLength = 1;
                    else static if (column.type == Column.Type.boolean         ) enum headerLength = 1;
                    else static if (column.type == Column.Type.uniqueidentifier) enum headerLength = 1;
                    else static if (column.type == Column.Type.rowversion      ) enum headerLength = 2;
                    else static if (column.type == Column.Type.binary          ) enum headerLength = 2;
                    else static if (column.type == Column.Type.varbinary       ) enum headerLength = 2;
                    else static if (column.type == Column.Type.image           ) enum headerLength = 4;
                    else static if (column.type == Column.Type.geography       ) enum headerLength = 8;
                    else static if (column.type == Column.Type.geometry        ) enum headerLength = 8;
                    else static if (column.type == Column.Type.hierarchyid     ) enum headerLength = 8;
                    else static if (column.type == Column.Type.variant         ) enum headerLength = 4;
                    else static if (column.type == Column.Type.fixedchar       ) enum headerLength = 2;
                    else static if (column.type == Column.Type.fixednchar      ) enum headerLength = 2;
                    else static if (column.type == Column.Type.varchar         ) enum headerLength = 2;
                    else static if (column.type == Column.Type.nvarchar        ) enum headerLength = 2;
                    else static if (column.type == Column.Type.varcharmax      ) enum headerLength = 8;
                    else static if (column.type == Column.Type.nvarcharmax     ) enum headerLength = 8;
                    else static if (column.type == Column.Type.text            ) enum headerLength = 4;
                    else static if (column.type == Column.Type.ntext           ) enum headerLength = 4;
                    else static if (column.type == Column.Type.xml             ) enum headerLength = 8;
                    
                    static if (!hasHeader)
                        enum isNull = false;
                    else
                    {
                             static if (headerLength == 1)  alias headerType = ubyte;
                        else static if (headerLength == 2)  alias headerType = ushort;
                        else static if (headerLength == 4)  alias headerType = uint;
                        else static if (headerLength == 8)  alias headerType = ulong;
                        else static assert (false, "Unexpected binary header length " ~ headerLength.to!string ~ ".");
                        
                        headerType length = consume!headerType;
                        
                        static if (column.isNullable)
                            auto isNull = length == headerType.max;
                        else
                            enum isNull = false;
                    }
                    
                    auto enforceLength(int expectedLength)
                    {
                        static if (hasHeader)
                            if (!isNull)
                                enforce(length == expectedLength, "A field had length " ~ length.to!string ~ " but was expected to be " ~ expectedLength.to!string ~ ".");
                    }
                    
                    static if (column.type == Column.Type.integer8)
                    {
                        enforceLength(1);
                        
                        if (!isNull)
                            __traits(getMember, record, column.fieldName) = consume!ubyte;
                    }
                    else static if (column.type == Column.Type.integer16)
                    {
                        enforceLength(2);
                        
                        if (!isNull)
                            __traits(getMember, record, column.fieldName) = consume!short;
                    }
                    else static if (column.type == Column.Type.integer32)
                    {
                        enforceLength(4);
                        
                        if (!isNull)
                            __traits(getMember, record, column.fieldName) = consume!int;
                    }
                    else static if (column.type == Column.Type.integer64)
                    {
                        enforceLength(8);
                        
                        if (!isNull)
                            __traits(getMember, record, column.fieldName) = consume!long;
                    }
                    else static if (column.type == Column.Type.float32)
                    {
                        enforceLength(4);
                        
                        if (!isNull)
                            __traits(getMember, record, column.fieldName) = consume!float;
                    }
                    else static if (column.type == Column.Type.float64)
                    {
                        enforceLength(8);
                        
                        if (!isNull)
                            __traits(getMember, record, column.fieldName) = consume!double;
                    }
                    else static if (column.type == Column.Type.boolean)
                    {
                        enforceLength(1);
                        if (!isNull)
                            __traits(getMember, record, column.fieldName) = consume!ubyte != 0;
                    }
                    else static if (column.type == Column.Type.rowversion)
                    {
                        enforceLength(8);
                        
                        if (!isNull)
                            __traits(getMember, record, column.fieldName) = consume!ulong;
                    }
                    else static if (column.type == Column.Type.decimal)
                    {
                        enforceLength(19);
                        
                        if (!isNull)
                        {
                            ubyte[19] data = void;
                            consumeBytes!19(data);
                            
                            auto lowBits = 
                                (cast(ulong)data[ 3] <<  0) | 
                                (cast(ulong)data[ 4] <<  8) | 
                                (cast(ulong)data[ 5] << 16) | 
                                (cast(ulong)data[ 6] << 24) | 
                                (cast(ulong)data[ 7] << 32) | 
                                (cast(ulong)data[ 8] << 40) | 
                                (cast(ulong)data[ 9] << 48) | 
                                (cast(ulong)data[10] << 56);
                            
                            auto highBits = 
                                (cast(ulong)data[11] <<  0) | 
                                (cast(ulong)data[12] <<  8) | 
                                (cast(ulong)data[13] << 16) | 
                                (cast(ulong)data[14] << 24) | 
                                (cast(ulong)data[15] << 32) | 
                                (cast(ulong)data[16] << 40) | 
                                (cast(ulong)data[17] << 48) | 
                                (cast(ulong)data[18] << 56);
                            
                            auto dec = Int128(lowBits, highBits);
                            
                            __traits(getMember, record, column.fieldName) = Decimal(
                                data[0], 
                                data[1], 
                                data[2] == 1, 
                                dec);
                        }
                    }
                    else static if (column.type == Column.Type.money)
                    {
                        enforceLength(8);
                        
                        if (!isNull)
                            __traits(getMember, record, column.fieldName) = consume!long;
                    }
                    else static if (column.type == Column.Type.smallmoney)
                    {
                        enforceLength(4);
                        
                        if (!isNull)
                            __traits(getMember, record, column.fieldName) = consume!int;
                    }
                    else static if (column.type == Column.Type.uniqueidentifier)
                    {
                        enforceLength(16);
                        
                        if (!isNull)
                        {
                            ubyte[16] data = void;
                            consumeBytes!16(data);
                            
                            // The BACPAC version uses a mixed endian GUID.
                            UUID value;
                            value.data[0] = data[3];
                            value.data[1] = data[2];
                            value.data[2] = data[1];
                            value.data[3] = data[0];
                            
                            value.data[4] = data[5];
                            value.data[5] = data[4];
                            
                            value.data[6] = data[7];
                            value.data[7] = data[6];
                            
                            value.data[8 .. 16] = data[8 .. 16];
                            
                            __traits(getMember, record, column.fieldName) = value;
                        }
                    }
                    else static if (
                        column.type == Column.Type.binary || 
                        column.type == Column.Type.varbinary || 
                        column.type == Column.Type.geography || 
                        column.type == Column.Type.geometry || 
                        column.type == Column.Type.image || 
                        column.type == Column.Type.hierarchyid || 
                        column.type == Column.Type.variant)
                    {
                        if (!isNull)
                            __traits(getMember, record, column.fieldName) = consumeUnknownBytes(length);
                    }
                    else static if (column.type == Column.Type.fixedchar)
                    {
                        if (!isNull)
                        {
                            ubyte[column.length * 2] data;
                            consumeBytes!(column.length * 2)(data);
                            
                            __traits(getMember, record, column.fieldName) =
                                (cast(ushort[])data).assumeUTF.to!string;
                            
                        }
                    }
                    else static if (
                        column.type == Column.Type.fixednchar || 
                        column.type == Column.Type.varchar || 
                        column.type == Column.Type.nvarchar || 
                        column.type == Column.Type.varcharmax || 
                        column.type == Column.Type.nvarcharmax || 
                        column.type == Column.Type.text || 
                        column.type == Column.Type.ntext || 
                        column.type == Column.Type.xml)
                    {
                        if (!isNull)
                            __traits(getMember, record, column.fieldName) =
                                (cast(ushort[])consumeUnknownBytes(length)).assumeUTF.to!string;
                    }
                    else static if (column.type == Column.Type.date)
                    {
                        // Type: date 1 byte length, 3 bytes: 6E 28 0B; 2003-02-01
                        enforceLength(3);
                        
                        if (!isNull)
                        {
                            ubyte[3] data;
                            consumeBytes!3(data);
                            uint days = 
                                (data[0] <<  0) | 
                                (data[1] <<  8) | 
                                (data[2] << 16);
                            
                            __traits(getMember, record, column.fieldName) = Date(1, 1, 1) + dur!"days"(days);
                        }
                    }
                    else static if (column.type == Column.Type.time && column.length == 0)
                    {
                        enforceLength(5);
                        
                        if (!isNull)
                        {
                            ubyte[5] data;
                            consumeBytes!5(data);
                            
                            ulong hectoNanoSeconds = 
                                (cast(ulong)data[0] <<  0) | 
                                (cast(ulong)data[1] <<  8) | 
                                (cast(ulong)data[2] << 16) |
                                (cast(ulong)data[3] << 24) | 
                                (cast(ulong)data[4] << 32);
                            __traits(getMember, record, column.fieldName) = TimeOfDay(0, 0, 0) + dur!"hnsecs"(hectoNanoSeconds);
                        }
                    }
                    else static if (
                        column.type == Column.Type.time || 
                        column.type == Column.Type.datetime2 || 
                        column.type == Column.Type.datetimeoffset)
                    {
                        // Type: time           1 byte length,  5 bytes: 80 E2 E3 59 5F;                           11:22:33.0000000
                        // Type: datetime2      1 byte length,  8 bytes: 07 B9 F6 59 5F 6E 28 0B;       2003-02-01 11:22:33:1234567
                        // Type: datetimeoffset 1 byte length, 10 bytes: 07 0F F1 23 36 6E 28 0B 27 01; 2003-02-01 11:22:33.1234567 +04:55
                        
                        if (!isNull)
                        {
                            static if (hasHeader)
                                enforce(length >= 5, "A field of type " ~ column.type.to!string ~ " in the BACPAC data had length " ~ length.to!string ~ " but was expected to be at least 5.");                                
                            
                            ubyte[5] timeData;
                            consumeBytes!5(timeData);
                            ulong hectoNanoSeconds = 
                                (cast(ulong)timeData[0] <<  0) | 
                                (cast(ulong)timeData[1] <<  8) | 
                                (cast(ulong)timeData[2] << 16) | 
                                (cast(ulong)timeData[3] << 24) | 
                                (cast(ulong)timeData[4] << 32);
                            auto timeComponent = dur!"hnsecs"(hectoNanoSeconds);
                            
                            static if (column.type != Column.Type.time)
                            {
                                ubyte[3] dateData;
                                consumeBytes!3(dateData);
                                uint days = 
                                    (dateData[0] <<  0) | 
                                    (dateData[1] <<  8) | 
                                    (dateData[2] << 16);
                            }
                            
                            static if (column.type == Column.Type.datetimeoffset)
                                auto offsetMinutes = consume!ushort;
                            
                            static if (column.type == Column.Type.time)
                                __traits(getMember, record, column.fieldName) = DateTime(1, 1, 1) + timeComponent;
                            else static if (column.type == Column.Type.datetime2)
                                __traits(getMember, record, column.fieldName) = DateTime(1, 1, 1) + dur!"days"(days) + timeComponent;
                            else static if (column.type == Column.Type.datetimeoffset)
                                __traits(getMember, record, column.fieldName) = DateTime(1, 1, 1) + dur!"days"(days) + timeComponent + dur!"minutes"(offsetMinutes);
                            else
                                static assert (false, "Static logic error.");
                        }
                    }
                    else static if (column.type == Column.Type.datetime)
                    {
                        // Type: datetime: 1 byte length, 8 bytes: 13 93 00 00 F1 77 BB 00; 2003-02-01 11:22:33.123
                        enforceLength(8);                         
                        if (!isNull)
                        {
                            // Days here can be negative for dates prior to 01-Jan-1900.
                            auto days = consume!int;
                            auto milliSecondTriplets = consume!int;
                            
                            __traits(getMember, record, column.fieldName) = DateTime(1900, 1, 1) + dur!"days"(days) + dur!"msecs"(milliSecondTriplets * 10 / 3);
                        }
                    }
                    else static if (column.type == Column.Type.smalldatetime)
                    {
                        // Type: smalldatetime 1 byte length, 4 bytes: 13 93 AB 02; 2003-02-01 11:23:00
                        enforceLength(4);
                        
                        if (!isNull)
                        {
                            auto days = consume!ushort;
                            auto minutes = consume!ushort;
                            
                            __traits(getMember, record, column.fieldName) = DateTime(1900, 1, 1) + dur!"days"(days) + dur!"minutes"(minutes);
                        }
                    }
                    else 
                        static assert (false, table.fullName, ".", column.fieldName, " has unsupported field type ", column.type, ".");
                }}
                
                records ~= record;
            }
        }
        
        return records.data;
    }
}

