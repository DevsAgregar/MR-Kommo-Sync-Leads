!macro NSIS_HOOK_POSTINSTALL
  DetailPrint "Aplicando ACLs restritivas na pasta do app..."
  nsExec::ExecToLog '"$SYSDIR\icacls.exe" "$INSTDIR" /inheritance:r /grant:r "$USERNAME:(OI)(CI)F" /grant:r "*S-1-5-18:(OI)(CI)F" /grant:r "*S-1-5-32-544:(OI)(CI)F" /T /C'
!macroend
