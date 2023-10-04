module generation;

import std.algorithm : any;
import std.array : array, appender, join;
import std.conv : to;
import std.file : write;
import std.path : buildNormalizedPath, withExtension;

import model;

auto generateStructModule(
    string moduleName, 
    const TableDefinition[] tables)
{
    auto code = appender!string;
    code.reserve(256 * 1024);
    code ~= "module " ~ moduleName ~ ";\n\n";
    
    if (tables.any!(table => table.columns.any!(column => column.isNullable)))
        code ~= "import std.typecons : Nullable;\n";
    
    if (tables.any!(table => table.columns.any!(column => column.type == "[uniqueidentifier]")))
        code ~= "import std.uuid : UUID;\n";
    
    auto dateTimeImports = appender!(string[]);
    dateTimeImports.reserve(7);
    
    if (tables.any!(table => table.columns.any!(column => column.type == "[date]")))
        dateTimeImports ~= "Date";
    
    if (tables.any!(table => table.columns.any!(column => column.type == "[time]")))
        dateTimeImports ~= "TimeOfDay";
    
    if (tables.any!(table => 
        table.columns.any!(column => 
            column.type == "[datetime]" || 
            column.type == "[datetime2]" || 
            column.type == "[datetimeoffset]" ||
            column.type == "[smalldatetime]")))
        dateTimeImports ~= "DateTime";
    
    if (dateTimeImports[].length > 0)
        code ~= "import std.datetime : " ~ join(dateTimeImports[], ", ") ~ ";\n";
    
    code ~= "import bacpac;\n\n";
    
    foreach (table; tables)
    {
        auto isInlineColumn(ColumnDefinition column) => 
            !column.isMax &&
               (column.type == "[char]"       ||
                column.type == "[nchar]"      ||
                column.type == "[varchar]"    ||
                column.type == "[nvarchar]"   ||
                column.type == "[binary]"     ||
                column.type == "[varbinary]");
        
        code ~= "@Table(\n";
        code ~= "    \"" ~ table.name ~ "\",\n";
        code ~= "    \"Data/" ~ table.dottedName ~ "/TableData-\",\n";
        code ~= "    [\n";
        
        foreach (index, column; table.columns)
        {
            string enumTypeName;
            switch (column.type)
            {
                case "[tinyint]":            enumTypeName = "integer8";          break;
                case "[smallint]":           enumTypeName = "integer16";         break;
                case "[int]":                enumTypeName = "integer32";         break;
                case "[bigint]":             enumTypeName = "integer64";         break;
                case "[real]":               enumTypeName = "float32";           break;
                case "[float]":              enumTypeName = "float64";           break;
                
                case "[decimal]":            enumTypeName = "decimal";           break;
                case "[numeric]":            enumTypeName = "decimal";           break;
                case "[smallmoney]":         enumTypeName = "smallmoney";        break;
                case "[money]":              enumTypeName = "money";             break;
                
                case "[date]":               enumTypeName = "date";              break;
                case "[datetime]":           enumTypeName = "datetime";          break;
                case "[datetime2]":          enumTypeName = "datetime2";         break;
                case "[datetimeoffset]":     enumTypeName = "datetimeoffset";    break;
                case "[smalldatetime]":      enumTypeName = "smalldatetime";     break;
                case "[time]":               enumTypeName = "time";              break;
                
                case "[bit]":                enumTypeName = "boolean";           break;
                case "[uniqueidentifier]":   enumTypeName = "uniqueidentifier";  break;
                case "[rowversion]":         enumTypeName = "rowversion";        break;
                case "[binary]":             enumTypeName = "binary";            break;
                case "[varbinary]":          enumTypeName = "varbinary";         break;
                case "[image]":              enumTypeName = "image";             break;
                case "[sys].[geography]":    enumTypeName = "geography";         break;
                case "[sys].[geometry]":     enumTypeName = "geometry";          break;
                case "[sys].[hierarchyid]":  enumTypeName = "hierarchyid";       break;
                case "[sql_variant]":        enumTypeName = "variant";           break;
                
                case "[char]":               enumTypeName = "fixedchar";         break;
                case "[nchar]":              enumTypeName = "fixednchar";        break;
                case "[varchar]":            enumTypeName = column.isMax ? "varcharmax"  : "varchar";  break;
                case "[nvarchar]":           enumTypeName = column.isMax ? "nvarcharmax" : "nvarchar"; break;
                case "[text]":               enumTypeName = "text";              break;
                case "[ntext]":              enumTypeName = "ntext";             break;
                case "[xml]":                enumTypeName = "xml";               break;
                default: 
                    throw new Exception("Unsupported column type " ~ column.type ~ ".");
            }
            
            auto length = -1;
            if (column.type == "[date]" || 
                column.type == "[datetime]" || 
                column.type == "[datetime2]" || 
                column.type == "[datetimeoffset]" || 
                column.type == "[smalldatetime]" || 
                column.type == "[time]")
            {
                length = column.scale;
            }
            else
            {
                length = column.length;
            }
            
            code ~= "        Column(\"";
            code ~= column.name;
            code ~= "\", \"";
            code ~= column.dFieldName;
            code ~= "\", Column.Type.";
            code ~= enumTypeName;
            
            if (column.isNullable || length > -1)
            {
                code ~= ", ";
                code ~= column.isNullable.to!string;
                
                if (length >= 0)
                {
                    code ~= ", ";
                    code ~= length.to!string;
                }
            }
            
            code ~= "),\n";
        }
        
        code ~= "    ])\n";
        code ~= "struct ";
        code ~= table.dStructName;
        code ~= "\n";
        code ~= "{\n";
        
        foreach (index, column; table.columns)
        {
            string dTypeName;
            auto isReference = false;
            
            switch (column.type)
            {
                case "[char]":
                case "[nchar]":
                case "[varchar]":
                case "[nvarchar]":
                case "[text]":
                case "[ntext]":
                case "[xml]":
                    dTypeName = "string";
                    isReference = true;
                    break;
                    
                case "[image]":
                case "[sql_variant]":
                case "[sys].[hierarchyid]":
                case "[sys].[geography]":
                case "[sys].[geometry]":
                case "[binary]":
                case "[varbinary]":
                    dTypeName = "const(ubyte)[]";
                    isReference = true;
                    break;

                case "[rowversion]":
                    dTypeName = "ulong";
                    break;
                    
                case "[tinyint]":
                    dTypeName = "ubyte";
                    break;
                    
                case "[smallint]":
                    dTypeName = "short";
                    break;
                    
                case "[int]":
                    dTypeName = "int";
                    break;
                    
                case "[bigint]":
                    dTypeName = "long";
                    break;
                    
                case "[float]": // TODO: There might be some translation required regarding the length.
                    dTypeName = "double";
                    break;
                    
                case "[real]": // Synonym for float(24) == C float.
                    dTypeName = "float";
                    break;
                    
                case "[bit]":
                    dTypeName = "bool";
                    break;
                    
                case "[decimal]":
                case "[numeric]":
                    dTypeName = "Decimal";
                    break;
                    
                case "[money]":
                    dTypeName = "long";
                    break;
                    
                case "[smallmoney]":
                    dTypeName = "int";
                    break;
                    
                case "[uniqueidentifier]":
                    dTypeName = "UUID";
                    break;
                    
                case "[date]":
                    dTypeName = "Date";
                    break;
                    
                case "[time]":
                    dTypeName = column.scale == 0 ? "TimeOfDay" : "DateTime";
                    break;
                    
                case "[smalldatetime]":
                case "[datetime]":
                case "[datetime2]":
                case "[datetimeoffset]":
                    dTypeName = "DateTime";
                    break;
                    
                default:
                    throw new Exception("Unsupported column type " ~ column.type ~ " in " ~ table.name ~ ".");
            }
            
            code ~= "    ";
            
            if (!column.isNullable || isReference)
                code ~= dTypeName;
            else
            {
                code ~= "Nullable!";
                code ~= dTypeName;
            }
            
            code ~= " ";
            code ~= column.dFieldName;
            code ~= ";\n";
        }
        
        code ~= "}\n\n";
    }
    
    return code[];
}

