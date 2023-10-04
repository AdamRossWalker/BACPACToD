module model;

import std.algorithm : countUntil, filter, substitute;
import std.array : appender;
import std.conv : to;
import std.stdio;
import std.string : lastIndexOf;
import std.typecons : Tuple;
import std.uni : isLower, toLower, toUpper;
import std.utf : byDchar;

import dxml.dom;
import dxml.parser;

alias Attribute = Tuple!(string, "name", string, "value", TextPos, "pos");
auto locationString(DOMEntity!string element) => element.pos.locationString;
auto locationString(Attribute attribute) => attribute.pos.locationString;
auto locationString(TextPos position) => "Line " ~ position.line.to!string ~ ", column " ~ position.col.to!string;

auto toCamelCase(string source)
{
    auto result = appender!string;
    auto index = source.byDchar.countUntil!isLower;
    
    if (index == 0)
        return source;
    else if (index == -1)
        return source.toLower.to!string;
    else if (1 < index && index < source.length)
        return source[0 .. index - 1].toLower.to!string ~ source[index - 1 .. $];
    else
        return source[0].toLower.to!string ~ source[1 .. $];
}

unittest
{
    assert("word".toCamelCase() == "word");
    assert("Word".toCamelCase() == "word");
    assert("WORD".toCamelCase() == "word");
    assert("firstSecond".toCamelCase() == "firstSecond");
    assert("FirstSecond".toCamelCase() == "firstSecond");
    assert("FirstSECOND".toCamelCase() == "firstSECOND");
    assert("FIRSTSecond".toCamelCase() == "firstSecond");
    assert("FSecond".toCamelCase() == "fSecond");
}

struct ColumnDefinition
{
    string name;
    bool isNullable;
    bool isMax;
    int length;
    int scale;
    int precision;
    string type;
    
    auto dFieldName() const pure => name[name.lastIndexOf('.') + 1 .. $].substitute("[", "", "]", "").to!string.toCamelCase;
}

struct TableDefinition
{
    string name;
    ColumnDefinition[] columns;
    
    auto dottedName() const pure => name.substitute("[", "", "]", "").to!string;
    
    auto dStructName() const pure => name.substitute("[", "", "]", "", ".", "").to!string.toCamelCase;
}

auto readTables(string xmlText, string bacpacFileName)
{
    auto rootElement = parseDOM(xmlText);

    auto tables = appender!(TableDefinition[]);
    foreach (table; rootElement.singleChild("DataSchemaModel").singleChild("Model").childrenNamed("Element"))
    {
        if (table.singleAttribute!false("Type") != "SqlTable")
            continue;
        
        const tableRawName = table.singleAttribute("Name");
        writeln("Reading table ", tableRawName, ".");
        tables ~= TableDefinition(tableRawName, table.readColumns(bacpacFileName, tableRawName));
    }
    
    return tables.data;
}

auto readColumns(DOMEntity!string tableElement, string bacpacFileName, string tableRawName)
{
    auto columns = appender!(ColumnDefinition[]);
    
    foreach (tableEntry; tableElement.singleChildWithAttribute("Relationship", "Name", "Columns").childrenNamed("Entry"))
    {
        foreach (columnElement; tableEntry.childrenNamed("Element"))
        {
            const columnRawName = columnElement.singleAttribute("Name");
            scope (failure) stderr.writeln("Error while reading column: ", columnRawName, ".");
            
            const columnCategory = columnElement.singleAttribute("Type");
            if (columnCategory != "SqlSimpleColumn")
            {
                writeWarning("Unsupported column type \"" ~ columnCategory ~ "\".", bacpacFileName, tableRawName, columnRawName, columnElement.pos);
                writeWarning("Currently only columns of type SqlSimpleColumn are supported.  Please raise a PR or an issue with an example BACPAC if you need this.");
                writeWarning("Skipping code generation for this table.");
                return null;
            }
            
            auto isNullable = true;
            auto length = -1;
            auto scale = 0;
            auto precision = 0;
            auto isMax = false;
            
            foreach (columnProperty; columnElement.childrenNamed("Property"))
            {
                const name = columnProperty.singleAttribute("Name");
                scope (failure) stderr.writeln("Error while reading column property: ", name, ".");
                
                if (name == "IsNullable")
                    isNullable = columnProperty.singleAttribute("Value").to!bool;
                else
                    writeWarning("Ignored unsupported column property \"" ~ name ~ "\".", bacpacFileName, tableRawName, columnRawName, columnProperty.pos);
            }
            
            auto typeElement = 
                columnElement
                .singleChildWithAttribute("Relationship", "Name", "TypeSpecifier")
                .singleChild("Entry")
                .singleChild("Element");
            
            auto columnTypeSpecifier = typeElement.singleAttribute("Type");
            if (columnTypeSpecifier != "SqlTypeSpecifier" &&
                columnTypeSpecifier != "SqlXmlTypeSpecifier")
            {
                writeWarning("Unsupported column type specifier \"" ~ columnTypeSpecifier ~ "\".", bacpacFileName, tableRawName, columnRawName, columnElement.pos);
                writeWarning("Currently only SqlTypeSpecifier and SqlXmlTypeSpecifier are supported.  Please raise a PR or an issue with an example BACPAC if you need this.");
                writeWarning("Skipping code generation for this table.");
                return null;
            }
            
            foreach (typeProperty; typeElement.childrenNamed("Property"))
            {
                const name = typeProperty.singleAttribute("Name");
                scope (failure) stderr.writeln("Error while reading type specifier property \"", name, "\".");
                
                const value = typeProperty.singleAttribute("Value");
                
                if (name == "Length")
                    length = value == "max" ? int.max : value.to!int;
                else if (name == "Scale")
                    scale = value.to!int;
                else if (name == "IsMax")
                    isMax = value.to!bool;
                else if (name == "Precision")
                    precision = value.to!int;
                else
                    writeWarning("Ignored unsupported type specifier property \"" ~ name ~ "\".", bacpacFileName, tableRawName, columnRawName, typeProperty.pos);
            }
            
            auto innerType = 
                typeElement
                .singleChildWithAttribute("Relationship", "Name", "Type")
                .singleChild("Entry")
                .singleChild("References");
            
            auto externalSource = innerType.singleAttribute!false("ExternalSource");
            if (externalSource.length > 0 && externalSource != "BuiltIns")
            {
                writeWarning("Unsupported column type ExternalSource \"" ~ externalSource ~ "\".", bacpacFileName, tableRawName, columnRawName, innerType.pos);
                writeWarning("Currently only columns of type ExternalSource=BuiltIns are supported.  Please raise a PR or an issue with an example BACPAC if you need this.");
                writeWarning("Skipping code generation for this table.");
                return null;
            }
            
            auto typeName = innerType.singleAttribute!false("Name");
            
            if (typeName != "[binary]"            && 
                typeName != "[bit]"               && 
                typeName != "[image]"             && 
                typeName != "[varbinary]"         && 
                
                typeName != "[char]"              && 
                typeName != "[nchar]"             && 
                typeName != "[text]"              && 
                typeName != "[ntext]"             && 
                typeName != "[varchar]"           && 
                typeName != "[nvarchar]"          && 
                
                typeName != "[date]"              && 
                typeName != "[datetime2]"         && 
                typeName != "[datetime]"          && 
                typeName != "[datetimeoffset]"    && 
                
                typeName != "[tinyint]"           && 
                typeName != "[smallint]"          && 
                typeName != "[int]"               && 
                typeName != "[bigint]"            && 
                typeName != "[float]"             && 
                typeName != "[real]"              && 
                typeName != "[decimal]"           && 
                typeName != "[money]"             && 
                typeName != "[smallmoney]"        && 
                typeName != "[numeric]"           && 
                typeName != "[uniqueidentifier]"  && 
                
                typeName != "[rowversion]"        && 
                
                typeName != "[time]"              && 
                typeName != "[smalldatetime]"     && 
                
                typeName != "[sys].[hierarchyid]" && 
                typeName != "[sys].[geography]"   && 
                typeName != "[sys].[geometry]"    && 
                typeName != "[sql_variant]"       && 
                
                typeName != "[xml]")
            {
                writeWarning("Unsupported column type \"" ~ typeName ~ "\".", bacpacFileName, tableRawName, columnRawName, innerType.pos);
                writeWarning("Please raise a PR or an issue with an example BACPAC if you need this.");
                writeWarning("Skipping code generation for this table.");
                return null;
            }
            
            columns ~= ColumnDefinition(columnRawName, isNullable, isMax, length, scale, precision, typeName);
        }
    }
    
    return columns.data;
}

auto writeWarning(
    string error, 
    string bacpacFileName = "", 
    string tableName = "", 
    string columnName = "", 
    TextPos position = TextPos.init)
{
    stderr.writeln(error);
    
    if (position != TextPos.init)
        stderr.writeln(position.locationString);
    
    if (bacpacFileName.length > 0)
        stderr.writeln("BACPAC file: ", bacpacFileName, ".");
    
    if (tableName.length > 0)
        stderr.writeln("Table: ", tableName, ".");
    
    if (columnName.length > 0)
        stderr.writeln("Column: ", columnName, ".");
}

auto childrenNamed(DOMEntity!string element, string childName) => 
    element.children.filter!(child => child.name == childName);

auto singleChild(bool isMandatory = true)(
    DOMEntity!string element, 
    string expectedName)
{
    auto requestedChildWasFound = false;
    DOMEntity!string result;
    foreach (child; element.children)
    {
        if (child.name != expectedName)
            continue;
        
        if (requestedChildWasFound)
            throw new Exception("Element \"" ~ expectedName ~ "\" occurred more than once.  This was expected to be unique.  " ~ child.locationString ~ ".");
        
        result = child;
        requestedChildWasFound = true;
    }
    
    static if (isMandatory)
        if (!requestedChildWasFound)
            throw new Exception("Mandatory element \"" ~ expectedName ~ "\" was not found.  " ~ element.locationString ~ ".");
    
    return result;
}

auto singleAttribute(bool isMandatory = true)(
    DOMEntity!string element, 
    string expectedName)
{
    auto requestedAttributeWasFound = false;
    auto value = "";
    foreach (attribute; element.attributes)
    {
        if (attribute.name != expectedName)
            continue;
        
        if (requestedAttributeWasFound)
            throw new Exception("Attribute \"" ~ expectedName ~ "\" occurred more than once.  This was expected to be unique.  The values are \"" ~ value ~ "\" and \"" ~ attribute.value ~ "\".  " ~ attribute.locationString ~ ".");
        
        value = attribute.value;
        requestedAttributeWasFound = true;
    }
    
    static if (isMandatory)
        if (!requestedAttributeWasFound)
            throw new Exception("Mandatory attribute " ~ expectedName ~ " was not found.  " ~ element.locationString ~ ".");
    
    return value;
}

auto singleChildWithAttribute(bool isMandatory = true)(
    DOMEntity!string element, 
    string expectedChildName, 
    string expectedAttributeName, 
    string expectedAttributeValue)
{
    auto requestedChildWasFound = false;
    DOMEntity!string result;
    foreach (child; element.children)
    {
        if (child.name != expectedChildName)
            continue;
        
        if (child.singleAttribute!false(expectedAttributeName) != expectedAttributeValue)
            continue;
        
        if (requestedChildWasFound)
            throw new Exception("Element \"" ~ expectedChildName ~ "\" with attribute \"" ~ expectedAttributeName ~ "\"=\"" ~ expectedAttributeValue ~ "\" occurred more than once.  This was expected to be unique.  " ~ child.locationString ~ ".");
        
        result = child;
        requestedChildWasFound = true;
    }
    
    static if (isMandatory)
        if (!requestedChildWasFound)
            throw new Exception("Mandatory element \"" ~ expectedChildName ~ "\" with attribute \"" ~ expectedAttributeName ~ "\"=\"" ~ expectedAttributeValue ~ "\" was not found.  " ~ element.locationString ~ ".");
    
    return result;
}
