module helpers;

import std.algorithm : map, max, min;
import std.conv : to;
import std.datetime : Date, DateTime, dur;
import std.exception : enforce;
import std.stdio : stderr, writeln;
import std.string : indexOf;

public auto stripSurroundingQuotes(string source)
{
    if (source.length > 1 && source[0] == '"' && source[$ - 1] == '"')
        return source[1 .. $ - 1];
    else
        return source;
}

public auto fromSqlServerToDate(string dateAsText)
{
    scope (failure) writeln("Error converting SQL Server date \"", dateAsText, "\".");
    
    auto source = dateAsText;
    
    if (source.length < 10)
    {
        writeln("Cannot convert date \"", source, "\".");
        return Date.init;
    }

    const year   = source[0 .. 4].to!int;     source = source[4 + 1 .. $];
    const month  = source[0 .. 2].to!int;     source = source[2 + 1 .. $];
    const day    = source[0 .. 2].to!int;     source = source[2     .. $];
    
    return Date(year, month, day);
}

public auto fromSqlServerToDateTime(string dateTimeAsText)
{
    scope (failure) writeln("Error converting SQL Server datetime \"", dateTimeAsText, "\".");
    
    auto source = dateTimeAsText;
    scope (failure) writeln("Processing section \"", source, "\".");
    
    if (source.length >= 8 && source[2] == ':')
    {
        const hour   = source[0 .. 2].to!int;  source = source[2 .. $];  source = source.stripLeading(':');
        const minute = source[0 .. 2].to!int;  source = source[2 .. $];  source = source.stripLeading(':');
        const second = source[0 .. 2].to!int;  source = source[2 .. $];
        
        ulong hectoNanoSeconds;
        if (source.length > 0)
        {
            if (source[0] == '.')
            {
                source = source.stripLeading('.');
                
                const spacePosition = source.indexOf(' ');
                const endOfFractionPosition = spacePosition < 0 ? source.length : spacePosition;
                 
                scope (failure) writeln("spacePosition = \"", spacePosition, "\".");
                
                hectoNanoSeconds = source[0 .. endOfFractionPosition].to!int;
            }
        }
        
        return DateTime(1, 1, 1, hour, minute, second) + dur!"hnsecs"(hectoNanoSeconds);
   }

    if (source.length < 19)
    {
        writeln("Cannot convert date \"", source, "\".");
        return DateTime.init;
    }

    const year   = source[0 .. 4].to!int;  source = source[4 .. $];  source = source.stripLeading('-');
    const month  = source[0 .. 2].to!int;  source = source[2 .. $];  source = source.stripLeading('-');
    const day    = source[0 .. 2].to!int;  source = source[2 .. $];  source = source.stripLeading(' ');
    const hour   = source[0 .. 2].to!int;  source = source[2 .. $];  source = source.stripLeading(':');
    const minute = source[0 .. 2].to!int;  source = source[2 .. $];  source = source.stripLeading(':');
    const second = source[0 .. 2].to!int;  source = source[2 .. $];
    
    ulong hectoNanoSeconds;
    int hoursOffset;
    int minutesOffset;
    
    if (source.length > 0)
    {
        if (source[0] == '.')
        {
            source = source.stripLeading('.');
            
            const spacePosition = source.indexOf(' ');
            const endOfFractionPosition = spacePosition < 0 ? source.length : spacePosition;
            
            scope (failure) writeln("spacePosition = \"", spacePosition, "\".");
            
            hectoNanoSeconds = source[0 .. endOfFractionPosition].to!int;
            source = source[endOfFractionPosition .. $];
        }
        
        source = source.stripLeading(' ');
    }
    
    if (source.length > 5)
    {
        hoursOffset   = source[0 .. 3].to!int;  source = source[3 .. $];  source = source.stripLeading(':');
        minutesOffset = source[0 .. 2].to!int;  source = source[2 .. $];  source = source.stripLeading(' ');
    }
    
    return DateTime(year, month, day, hour, minute, second) + dur!"hnsecs"(hectoNanoSeconds);
}

private string stripLeading(string source, char character)
{
    if (source.length == 0)
        return null;
    
    enforce(source[0] == character, "Expected \"" ~ character ~ "\", but found \"" ~ source ~ "\".");
    
    return source[1 .. $];
}

unittest
{
    assert (fromSqlServerToDate("2003-01-02")                       == Date(2003, 1, 2));
    assert (fromSqlServerToDateTime("2003-01-02 04:05:06")          == DateTime(2003, 1, 2, 4, 5, 6));
    assert (fromSqlServerToDateTime("2003-01-02 04:05:06.1")        == DateTime(2003, 1, 2, 4, 5, 6) + dur!"hnsecs"(1000000));
    assert (fromSqlServerToDateTime("2003-01-02 04:05:06.1234567")  == DateTime(2003, 1, 2, 4, 5, 6) + dur!"hnsecs"(1234567));
    assert (fromSqlServerToDateTime("04:05:06")                     == DateTime(1, 1, 1, 4, 5, 6));
    assert (fromSqlServerToDateTime("04:05:06.1")                   == DateTime(1, 1, 1, 4, 5, 6) + dur!"hnsecs"(1000000));
    assert (fromSqlServerToDateTime("04:05:06.1234567")             == DateTime(1, 1, 1, 4, 5, 6) + dur!"hnsecs"(1234567));
}
