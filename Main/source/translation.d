module translation;

import std.array : appender;
import std.conv : to;
import std.encoding : BOM, EncodingException, EncodingScheme, getBOM, INVALID_SEQUENCE;

auto translateBytesToString(const(ubyte)[] data) @trusted
{
    const bom = data.getBOM;
    data = data[bom.sequence.length .. $];
    
    auto encoding = "UTF-8";
    switch (bom.schema)
    {
        case BOM.utf32be: encoding = "UTF-32BE"; break;
        case BOM.utf32le: encoding = "UTF-32LE"; break;
        case BOM.utf16be: encoding = "UTF-16BE"; break;
        case BOM.utf16le: encoding = "UTF-16LE"; break;
        default : break;
    }
    
    const encodingScheme = EncodingScheme.create(encoding);
    
    auto text = appender!string;
    auto invalidCharactersFound = false;
    while (data.length != 0)
    {
        auto character = encodingScheme.safeDecode(data);
        if (character == INVALID_SEQUENCE)
        {
            invalidCharactersFound |= true;
            continue;
        }
        
        text ~= character;
    }
    
    if (invalidCharactersFound)
        throw new Exception(
            "File included an unexpected encoding.\n" ~
            "The BOM (not mandatory) was " ~ bom.to!string ~ ".\n" ~
            "Attempted decoding using " ~ encoding ~ ".");
    
    return text.data;
}
