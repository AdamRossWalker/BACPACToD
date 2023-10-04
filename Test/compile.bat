@ECHO OFF

REM Clear the output.
TYPE NUL > compiler_output.txt

ECHO Running BACPACToD to get generated code...

..\Main\bin\BACPACToD.exe --output_directory source 2>> compiler_output.txt

IF %ERRORLEVEL% NEQ 0 GOTO ERRORS

REM Use this if there are any changes to resources.rc.
REM resources\RC\rc.exe resources\resources.rc 2>> compiler_output.txt

ECHO Building Test project...

dub.exe build --quiet --arch=x86_64 2>> compiler_output.txt
REM dub.exe build --build=release --quiet --arch=x86_64 2>> compiler_output.txt

IF %ERRORLEVEL% NEQ 0 GOTO ERRORS

    ECHO Running...

    .\bin\BACPACToDTest.exe --output_directory source 2>> compiler_output.txt
    
IF %ERRORLEVEL% NEQ 0 GOTO ERRORS

    REM pause
    dub.exe test --quiet --arch=x86_64 2>> compiler_output.txt

IF %ERRORLEVEL% NEQ 0 GOTO ERRORS

GOTO END

:ERRORS

compiler_output.txt

:END
pause;
