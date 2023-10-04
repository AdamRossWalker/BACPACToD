module app;

import std.algorithm : map, max, min;
import std.array : array, appender;
import std.conv : to;
import std.csv : csvReader;
import std.digest : toHexString;
import std.datetime : Date, DateTime;
import std.file : readText, write;
import std.format : format;
import std.math : isClose;
import std.meta : Filter;
import std.path : buildNormalizedPath, withExtension;
import std.stdio : stderr, writeln;
import std.string : toUpper, strip;
import std.traits : getUDAs, isCallable, isInstanceOf, isType, Unqual;
import std.typecons : Nullable;
import std.uuid : UUID;

import bacpac;
import table_definitions;
import helpers;

auto main(string[] arguments)
{
    auto sourceBacpac = new Bacpac("Test.bacpac", log => stderr.writeln(log));
    
    auto results = appender!string;
    
    auto tableCount = 0;
    auto totalChecks = 0;
    auto totalFailures = 0;
    
    static foreach (memberName; __traits(allMembers, table_definitions))
    {{
        alias TRecord = __traits(getMember, table_definitions, memberName);
        
        static if (isType!TRecord)
        {
            enum tableAttributes = getUDAs!(TRecord, Table);
            static assert (tableAttributes.length == 1, "Record type ", table.name, " should have the Table attribute exactly once.");
            enum table = tableAttributes[0];
            
            tableCount++;
            writeln(memberName);
            
            enum isField(string memberName) =
                !__traits(compiles, { enum _ = __traits(getMember, TRecord, memberName); }) && 
                !isCallable!(__traits(getMember, TRecord, memberName));    
            
            enum members = Filter!(isField, __traits(allMembers, TRecord));
            
            results ~= TRecord.stringof;
            results ~= "\n";
            
            static foreach (index, column; table.columns)
            {
                if (index > 0)
                    results ~= ",";
                
                results ~= column.fieldName;
            }
            
            auto bacpacRecords = sourceBacpac.readFullTable!TRecord;
            
            results ~= "\n";
            foreach (record; bacpacRecords)
            {
                static foreach (index, column; table.columns)
                {{
                    if (index > 0)
                        results ~= ",";
                    
                    alias type = typeof(__traits(getMember, record, column.fieldName));
                    
                    static if (isInstanceOf!(Nullable, type))
                        const isNull = __traits(getMember, record, column.fieldName).isNull;
                    else
                        enum isNull = false;
                    
                    if (isNull)
                        results ~= "NULL";
                    else
                    {
                        static if (is(type : Nullable!bool))
                            results ~= __traits(getMember, record, column.fieldName).get ? "1" : "0";
                        else static if (is(type : const(bool)))
                            results ~= __traits(getMember, record, column.fieldName) ? "1" : "0";
                        else
                            results ~= __traits(getMember, record, column.fieldName).to!string;
                    }
                }}
                
                results ~= "\n";
            }
            
            results ~= "\n";
            
            auto checkMatch(T)(Column column, int recordIndex, T actualValue, T expectedValue)
            {
                totalChecks++;
            
                static if (is(T : double))
                {
                    if (isClose(actualValue, expectedValue))
                        return;
                }
                else
                {
                    if (actualValue == expectedValue)
                        return;
                }
                
                totalFailures++;
                writeln("Table ", TRecord.stringof, " on record ", recordIndex, " has field ", column.fieldName, " with value \"", actualValue, "\" but \"", expectedValue, "\" was expected.");
            }
            
            auto csvRecords = 
                buildNormalizedPath(".\\test_data", withExtension(memberName, ".csv").array)
                .readText
                .strip("\r\n")
                .csvReader
                .map!(record => record.array)
                .array;
            
            auto csvRecordIndex = 0;
            static foreach (fieldIndex, column; table.columns)
                checkMatch(column, csvRecordIndex, column.fieldName.toUpper, csvRecords[0][fieldIndex].toUpper);
            
            foreach (record; bacpacRecords)
            {
                csvRecordIndex++;
                
                totalChecks++;
                if (csvRecordIndex >= csvRecords.length)
                {
                    totalFailures++;
                    writeln("Table ", TRecord.stringof, " has extra records not found in the CSV file.");
                    break;
                }
                
                static foreach (fieldIndex, column; table.columns)
                {{
                    const baseBacpacField = __traits(getMember, record, column.fieldName);
                    const csvField = csvRecords[csvRecordIndex][fieldIndex].stripSurroundingQuotes;
                    
                    enum isNullable = isInstanceOf!(Nullable, typeof(baseBacpacField));
                    
                    static if (isNullable)
                        const isNull = __traits(getMember, record, column.fieldName).isNull;
                    else
                        enum isNull = false;
                    
                    if (isNull)
                        checkMatch(column, csvRecordIndex, "NULL", csvField);
                    else
                    {
                        static if (isNullable)
                            const bacpacField = baseBacpacField.get;
                        else
                            const bacpacField = baseBacpacField;
                        
                        alias bacpacFieldType = Unqual!(typeof(bacpacField));
                        
                        static if (column.type == Column.Type.decimal || 
                                   column.type == Column.Type.money || 
                                   column.type == Column.Type.smallmoney || 
                                   column.type == Column.Type.variant)
                        {
                            // TODO: These column types are not yet fully implemented.
                        }
                        else static if (column.type == Column.Type.rowversion)
                        {
                            ubyte[8] data = *cast(ubyte[8]*)&bacpacField;
                            checkMatch(column, csvRecordIndex, "0x" ~ data.toHexString.to!string, csvField);
                        }
                        else static if (is(bacpacFieldType : const(ubyte)[]))
                        {
                            const csvFieldWithoutNull = csvField == "NULL" ? "0x" : csvField;
                            checkMatch(column, csvRecordIndex, "0x" ~ bacpacField.toHexString, csvFieldWithoutNull);
                        }
                        else static if (is(bacpacFieldType : string))
                        {
                            const csvFieldWithoutNull = csvField == "NULL" ? "" : csvField;
                            checkMatch(column, csvRecordIndex, bacpacField, csvFieldWithoutNull);
                        }
                        else static if (is(bacpacFieldType : bool))
                            checkMatch(column, csvRecordIndex, bacpacField, csvField == "1");
                        else static if (is(bacpacFieldType == float))
                            checkMatch(column, csvRecordIndex, bacpacField, csvField.to!float);
                        else static if (is(bacpacFieldType == double))
                            checkMatch(column, csvRecordIndex, bacpacField, csvField.to!double);
                        else static if (is(bacpacFieldType : Date))
                            checkMatch(column, csvRecordIndex, bacpacField, csvField.fromSqlServerToDate);
                        else static if (is(bacpacFieldType : DateTime))
                            checkMatch(column, csvRecordIndex, bacpacField, csvField.fromSqlServerToDateTime);
                        else static if (is(bacpacFieldType : UUID))
                            checkMatch(column, csvRecordIndex, bacpacField.to!string.toUpper, csvField.toUpper);
                        else static if (is(bacpacFieldType : string))
                            checkMatch(column, csvRecordIndex, bacpacField, csvField);
                        else 
                            checkMatch(column, csvRecordIndex, bacpacField.to!string, csvField);
                    }
                }}
            }
            
            totalChecks++;
            if (csvRecordIndex < csvRecords.length - 1)
            {
                totalFailures++;
                writeln("Table ", TRecord.stringof, " has extra records in the CSV file that were not found in the BACPAC.");
            }
        }
    }}
    
    write("extracted_bacpac_tables.txt", results[]);
    
    if (tableCount == 0)
        writeln("No tables found in table_definitions.d!");
    else
        writeln("Compared ", tableCount, " tables.");
    
    if (totalChecks > 0)
        writeln("Successful checks: ", totalChecks - totalFailures, " of ", totalChecks, ".  Pass rate: ", format("%3.2f%%", 100.0 * (totalChecks - totalFailures) / totalChecks));
    
    return 0;
}
