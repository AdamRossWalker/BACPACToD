@ECHO OFF

REM Clear the output.
TYPE NUL > compiler_output.txt

REM Use this if there are any changes to resources.rc.
REM resources\RC\rc.exe resources\resources.rc 2>> compiler_output.txt

REM dub.exe build --quiet --arch=x86_64 2>> compiler_output.txt
dub.exe build --build=release --quiet --arch=x86_64 2>> compiler_output.txt

IF %ERRORLEVEL% NEQ 0 GOTO ERRORS

    dub.exe test --quiet --arch=x86_64 2>> compiler_output.txt

IF %ERRORLEVEL% NEQ 0 GOTO ERRORS

GOTO END

:ERRORS

compiler_output.txt

:END
