module app;

import std.array : array, appender;
import std.conv : to;
import std.getopt;
import std.file : dirEntries, read, SpanMode, write;
import std.path : asNormalizedPath, baseName, buildNormalizedPath, dirName, stripExtension, withExtension;
import std.regex : regex, replaceAll;
import std.stdio;

import bacpac;
import model;
import translation;
import generation;

auto main(string[] arguments)
{
    // These codes are referenced in usage.txt, so please keep them in sync.
    enum ReturnCode
    {
        successful     = 0,
        helpRequested  = 1,
        noFilesFound   = 2,
        generalFailure = 3,
    }
    
    auto parameterInputFilter = "*.bacpac";
    auto parameterOutputDirectory = "";
    auto moduleName = "";
    auto helpWanted = false;
    
    try
        helpWanted = getopt(
            arguments,
            "input", &parameterInputFilter, 
            "output_directory", &parameterOutputDirectory, 
            "module_name", &moduleName).helpWanted;
    catch (GetOptException exception)
    {
        writeln(exception.message);
        helpWanted = true;
    }
    
    if (helpWanted)
    {
        writeln(import("usage.txt"));
        return ReturnCode.helpRequested;
    }
    
    const inputFilter = parameterInputFilter.asNormalizedPath.to!string;
    const sourceDirectory = inputFilter.dirName.to!string;
    const targetDirectory = parameterOutputDirectory.asNormalizedPath.to!string;
    const fileNameFilter = inputFilter.baseName.to!string;
    const sourceFiles = dirEntries(sourceDirectory, fileNameFilter, SpanMode.shallow).array;
    
    if (moduleName.length == 0)
        moduleName = "table_definitions";
    
    auto logger(string text) => stderr.writeln(text);
    
    auto fileCount = 0;
    try
    {
        foreach (string bacpacFileName; sourceFiles)
        {
            fileCount++;
            
            writeln("Reading source BACPAC: ", bacpacFileName);
            scope (failure) logger("Error while reading BACPAC: " ~ bacpacFileName ~ ".");
            
            auto bacpac = new Bacpac(bacpacFileName, &logger);
            
            auto modelBytes = bacpac.readInnerFile("model.xml");
            scope (failure) logger("Error while reading BACPAC file model.xml.");
            
            if (modelBytes.length == 0)
                throw new Exception("File model.xml in BACPAC " ~ bacpacFileName ~ " was empty.");
            
            const modelXml = modelBytes.translateBytesToString;
            const tables = readTables(modelXml, bacpacFileName);
            
            if (tables.length == 0)
                throw new Exception("No tables were found in " ~ bacpacFileName ~ ".");
            
            const thisModuleName = 
                sourceFiles.length == 1 ? 
                moduleName : 
                moduleName ~ "_" ~ stripExtension(baseName(bacpacFileName)).replaceAll(regex("[^0-9a-zA-Z]"), "");
            
            const moduleData = generateStructModule(thisModuleName, tables);
            const moduleFilename = buildNormalizedPath(targetDirectory, moduleName.withExtension("d").array);
            
            write(moduleFilename, moduleData);
            writeln("BACPAC module file created: ", moduleFilename);
        }
    }
    catch (Exception exception)
    {
        writeln(exception.message);
        return ReturnCode.generalFailure;
    }
    
    if (fileCount == 0)
    {
        writeln("No BACPAC files found.  Please try --help for more information.");
        return ReturnCode.noFilesFound;
    }
    
    const bacpacAccessFile = buildNormalizedPath(targetDirectory, "bacpac.d");
    write(bacpacAccessFile, import("bacpac.d"));
    writeln("BACPAC access file created: ", bacpacAccessFile);
    
    writeln(fileCount, " BACPAC file", (fileCount == 1 ? "" : "s"), " successfully processed.");
    
    return ReturnCode.successful;
}
